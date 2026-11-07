package dev.wawlink.app

import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/** A notification opens an existing session without modifying its draft or runtime identity. */
class TaskNotificationNavigation {
    data class Request(val sessionId: String, val sequence: Long)

    private val sequence = AtomicLong()
    private val pending = MutableStateFlow<Request?>(null)
    val request: StateFlow<Request?> = pending.asStateFlow()
    private val visible = MutableStateFlow<String?>(null)
    val visibleSession: StateFlow<String?> = visible.asStateFlow()

    fun openSession(sessionId: String) {
        if (sessionId.isNotBlank()) pending.value = Request(sessionId, sequence.incrementAndGet())
    }

    fun handled(request: Request) {
        pending.compareAndSet(request, null)
    }

    fun showSession(sessionId: String?) {
        visible.value = sessionId
    }
}
