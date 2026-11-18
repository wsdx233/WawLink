package dev.wawlink.driver

import java.io.IOException
import kotlin.test.*

class OmpRpcReliabilityTest {
    @Test fun onlyConsecutiveSilentProbesDeclareDisconnection() {
        val liveness = OmpLiveness()
        val initial = liveness.probeStarted()
        assertFalse(liveness.timedOut(initial))
        assertFalse(liveness.timedOut(initial))
        liveness.received()
        assertFalse(liveness.timedOut(initial))
        val afterFrame = liveness.probeStarted()
        assertFalse(liveness.timedOut(afterFrame))
        assertFalse(liveness.timedOut(afterFrame))
        assertTrue(liveness.timedOut(afterFrame))
        liveness.answered()
        assertFalse(liveness.timedOut(afterFrame))
    }

    @Test fun ioDiagnosticsKeepRealCausesButRemoveCommonCredentials() {
        val failure = IOException("write failed at /private/session.jsonl", IOException(
            "Permission denied; api_key=private-key Authorization: Bearer private-bearer https://user:pass@example.test/log?credential=private-query sk-private-token",
        ))
        val diagnostic = ompIoDiagnostic(failure)
        assertContains(diagnostic, "write failed at /private/session.jsonl")
        assertContains(diagnostic, "IOException: Permission denied")
        for (secret in listOf("private-key", "private-bearer", "user:pass", "private-query", "sk-private-token")) {
            assertFalse(diagnostic.contains(secret), diagnostic)
        }
    }
}
