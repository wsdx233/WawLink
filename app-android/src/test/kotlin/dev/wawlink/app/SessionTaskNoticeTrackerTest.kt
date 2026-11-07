package dev.wawlink.app

import dev.wawlink.protocol.ApprovalCardItem
import dev.wawlink.protocol.ApprovalDecision
import dev.wawlink.protocol.AskAnswer
import dev.wawlink.protocol.AskCardItem
import dev.wawlink.protocol.AskQuestion
import dev.wawlink.protocol.ChatSessionUiState
import dev.wawlink.protocol.SessionStatus
import dev.wawlink.protocol.StepGroupNodeItem
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class SessionTaskNoticeTrackerTest {
    @Test fun historicalCompletionAndIntentionalStopDoNotNotify() {
        val tracker = SessionTaskNoticeTracker()
        assertNull(tracker.update(state(status = SessionStatus.DONE)))
        assertNull(tracker.update(state(status = SessionStatus.RUNNING)))
        assertNull(tracker.update(state(status = SessionStatus.STOPPED)))
        assertNull(tracker.update(state(status = SessionStatus.DONE)))
    }

    @Test fun completionNotifiesOnceForEachObservedRun() {
        val tracker = SessionTaskNoticeTracker()
        assertNull(tracker.update(state(status = SessionStatus.RUNNING)))
        assertEquals(SessionTaskNotice.COMPLETED, tracker.update(state(status = SessionStatus.DONE)))
        assertNull(tracker.update(state(status = SessionStatus.DONE, elapsedSec = 12)))
        assertNull(tracker.update(state(status = SessionStatus.RUNNING)))
        assertEquals(SessionTaskNotice.COMPLETED, tracker.update(state(status = SessionStatus.DONE)))
    }

    @Test fun nestedAsksAndApprovalsUseTheirOwnPendingIdentity() {
        val tracker = SessionTaskNoticeTracker()
        val ask = AskCardItem("ask-1", "选择分支", listOf(AskQuestion("q1", "继续吗")))
        val approval = ApprovalCardItem("approval-1", "需要批准", "", "")
        assertNull(tracker.update(state(status = SessionStatus.RUNNING)))
        val waiting = state(status = SessionStatus.WAITING, feed = listOf(StepGroupNodeItem("todo", "任务", children = listOf(ask))))
        assertEquals(SessionTaskNotice.ANSWER, tracker.update(waiting))
        assertNull(tracker.update(waiting.copy(elapsedSec = 13)))
        val answered = ask.copy(answers = listOf(AskAnswer("q1", customInput = "继续")))
        assertEquals(SessionTaskNotice.APPROVAL, tracker.update(waiting.copy(feed = listOf(answered, approval))))
        assertNull(tracker.update(waiting.copy(feed = listOf(answered, approval.copy(resolution = ApprovalDecision.ALLOW_ONCE)))))
        assertEquals(SessionTaskNotice.ANSWER, tracker.update(waiting.copy(feed = listOf(ask.copy(id = "ask-2")))))
    }

    @Test fun failuresNotifyWithoutRepeatingStreamingUpdatesOrHistoricErrors() {
        val tracker = SessionTaskNoticeTracker()
        val failure = state(status = SessionStatus.ERROR, error = "认证失败")
        assertNull(tracker.update(failure))
        assertNull(tracker.update(state(status = SessionStatus.RUNNING)))
        assertEquals(SessionTaskNotice.FAILED, tracker.update(failure))
        assertNull(tracker.update(failure.copy(elapsedSec = 10)))
        assertEquals(SessionTaskNotice.FAILED, tracker.update(failure.copy(error = "网络连接失败")))
        assertNull(tracker.update(failure.copy(status = SessionStatus.STOPPED, error = "已停止")))
    }

    @Test fun sessionDeletionAndIndependentSessionsDoNotMixTransitions() {
        val tracker = SessionTaskNoticeTracker()
        assertNull(tracker.update(state(id = "first", status = SessionStatus.RUNNING)))
        assertNull(tracker.update(state(id = "second", status = SessionStatus.DONE)))
        assertEquals(SessionTaskNotice.COMPLETED, tracker.update(state(id = "first", status = SessionStatus.DONE)))
        tracker.retain(setOf("second"))
        assertNull(tracker.update(state(id = "first", status = SessionStatus.DONE)))
    }

    private fun state(
        id: String = "session",
        status: SessionStatus,
        feed: List<dev.wawlink.protocol.FeedItem> = emptyList(),
        error: String? = null,
        elapsedSec: Int = 0,
    ) = ChatSessionUiState(id, status = status, feed = feed, error = error, elapsedSec = elapsedSec)
}
