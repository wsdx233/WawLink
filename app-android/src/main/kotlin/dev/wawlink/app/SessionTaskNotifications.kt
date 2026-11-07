package dev.wawlink.app

import android.Manifest
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import dev.wawlink.protocol.ApprovalCardItem
import dev.wawlink.protocol.AskCardItem
import dev.wawlink.protocol.ChatSessionUiState
import dev.wawlink.protocol.FeedItem
import dev.wawlink.protocol.SessionStatus
import dev.wawlink.protocol.StepGroupNodeItem
import dev.wawlink.ui.redactErrorDetails
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.launch

internal enum class SessionTaskNotice(val title: String) {
    ANSWER("需要回答"), APPROVAL("需要批准"), COMPLETED("任务已完成"), FAILED("任务遇到错误"),
}

/** Watches only already-observed session state. It never restores historical controllers. */
internal class SessionTaskNoticeTracker {
    private data class Snapshot(val status: SessionStatus, val error: String?, val waiting: List<Pair<String, SessionTaskNotice>>) {
        val running get() = status == SessionStatus.RUNNING || status == SessionStatus.WAITING
    }
    private val previous = mutableMapOf<String, Snapshot>()

    fun update(state: ChatSessionUiState): SessionTaskNotice? {
        val waiting = mutableListOf<Pair<String, SessionTaskNotice>>()
        if (state.status == SessionStatus.WAITING) {
            fun inspect(items: List<FeedItem>) {
                for (item in items) when (item) {
                    is AskCardItem -> if (!item.isResolved) waiting += item.id to SessionTaskNotice.ANSWER
                    is ApprovalCardItem -> if (item.resolution == null) waiting += item.id to SessionTaskNotice.APPROVAL
                    is StepGroupNodeItem -> inspect(item.children)
                    else -> Unit
                }
            }
            inspect(state.feed)
        }
        val current = Snapshot(state.status, state.error, waiting)
        val before = previous.put(state.id, current) ?: return null
        return when {
            current.error != null && current.error != before.error && current.status != SessionStatus.STOPPED -> SessionTaskNotice.FAILED
            current.waiting.isNotEmpty() && current.waiting != before.waiting ->
                if (current.waiting.any { it.second == SessionTaskNotice.ANSWER }) SessionTaskNotice.ANSWER else SessionTaskNotice.APPROVAL
            before.running && current.status == SessionStatus.DONE && current.error == null -> SessionTaskNotice.COMPLETED
            else -> null
        }
    }

    fun retain(sessionIds: Set<String>) { previous.keys.retainAll(sessionIds) }
}

/** Task alerts have their own channel and IDs; the foreground-service contract is untouched. */
internal class SessionTaskNotifications(
    private val context: Context,
    private val coordinator: AppCoordinator,
    scope: CoroutineScope,
) {
    private val manager = context.getSystemService(NotificationManager::class.java)
    private val tracker = SessionTaskNoticeTracker()
    private val posted = mutableMapOf<String, SessionTaskNotice>()
    private var visibleSessionId: String? = null
    private var available = true

    init {
        try {
            manager.createNotificationChannel(NotificationChannel(CHANNEL, "会话任务", NotificationManager.IMPORTANCE_DEFAULT).apply {
                description = "任务完成、失败及等待回答或批准"
                lockscreenVisibility = Notification.VISIBILITY_PRIVATE
            })
            // Android can retain alerts after process death. Preference changes and deleted
            // sessions must clear those too, without touching the foreground-service notice.
            for (active in manager.activeNotifications) {
                val tag = active.tag ?: continue
                if (active.notification.channelId != CHANNEL || !tag.startsWith(TAG_PREFIX)) continue
                val kind = active.notification.extras.getString(KIND)
                val event = SessionTaskNotice.entries.firstOrNull { it.name == kind } ?: continue
                posted[tag.removePrefix(TAG_PREFIX)] = event
            }
        } catch (failure: Exception) {
            available = false
            coordinator.showError(IllegalStateException("任务通知初始化失败", failure))
        }
        scope.launch {
            combine(coordinator.sessions, coordinator.taskNotifications) { sessions, enabled -> sessions to enabled }.collect { (sessions, enabled) ->
                if (!available) return@collect
                val ids = sessions.mapTo(mutableSetOf()) { it.record.id }
                tracker.retain(ids)
                (posted.keys - ids).forEach(::cancel)
                for (session in sessions) {
                    val state = session.state
                    val event = tracker.update(state)
                    if (!enabled || session.record.id == visibleSessionId) {
                        cancel(session.record.id)
                        continue
                    }
                    val current = posted[session.record.id]
                    if (current == SessionTaskNotice.ANSWER || current == SessionTaskNotice.APPROVAL) {
                        if (state.status != SessionStatus.WAITING) cancel(session.record.id)
                    } else if (state.status == SessionStatus.RUNNING && current != null) cancel(session.record.id)
                    if (event != null) show(session, event)
                }
            }
        }
    }

    fun showSession(sessionId: String?) {
        visibleSessionId = sessionId
        sessionId?.let(::cancel)
    }

    private fun cancel(sessionId: String) {
        if (posted.remove(sessionId) != null) try {
            manager.cancel(tag(sessionId), NOTIFICATION_ID)
        } catch (failure: Exception) {
            coordinator.showError(IllegalStateException("无法清除任务通知", failure))
        }
    }

    private fun show(session: HomeSession, event: SessionTaskNotice) {
        if (Build.VERSION.SDK_INT >= 33 && ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return
        if (!manager.areNotificationsEnabled()) return
        val id = session.record.id
        val intent = Intent(context, MainActivity::class.java).apply {
            action = OPEN_SESSION
            data = Uri.Builder().scheme("wawlink").authority("session-task").appendPath(id).build()
            putExtra(SESSION_ID, id)
            addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        }
        val pending = PendingIntent.getActivity(context, 0, intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        val publicVersion = NotificationCompat.Builder(context, CHANNEL)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle("WawLink")
            .setContentText(event.title)
            .build()
        val notification = NotificationCompat.Builder(context, CHANNEL)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(event.title)
            .setContentText(redactErrorDetails(session.record.title).take(160))
            .setContentIntent(pending)
            .setAutoCancel(true)
            .setOnlyAlertOnce(false)
            .setVisibility(NotificationCompat.VISIBILITY_PRIVATE)
            .setPublicVersion(publicVersion)
            .addExtras(Bundle().apply { putString(KIND, event.name) })
            .setCategory(if (event == SessionTaskNotice.ANSWER || event == SessionTaskNotice.APPROVAL) NotificationCompat.CATEGORY_MESSAGE else NotificationCompat.CATEGORY_STATUS)
            .build()
        try {
            manager.notify(tag(id), NOTIFICATION_ID, notification)
            posted[id] = event
        } catch (_: SecurityException) {
            // Permission may be revoked after the explicit check; it cannot affect the task.
        } catch (failure: Exception) {
            coordinator.showError(IllegalStateException("无法显示任务通知", failure))
        }
    }

    companion object {
        internal const val CHANNEL = "wawlink-session-tasks"
        internal const val OPEN_SESSION = "dev.wawlink.app.OPEN_SESSION_TASK"
        internal const val SESSION_ID = "wawlink.session.id"
        private const val KIND = "wawlink.task.kind"
        private const val TAG_PREFIX = "session-task:"
        private const val NOTIFICATION_ID = 2
        private fun tag(sessionId: String) = "$TAG_PREFIX$sessionId"

        fun sessionFrom(intent: Intent?): String? = intent?.takeIf { it.action == OPEN_SESSION }
            ?.getStringExtra(SESSION_ID)?.takeIf { it.isNotBlank() }
    }
}
