package com.particledb;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class PipelineReadinessTest {
    @Test
    void pipelineDefinitionUsesSupportedDdlAndValidatesInputs() {
        String port = System.getenv("PDB_NATIVE_PORT");
        assumeTrue(port != null, "PDB_NATIVE_PORT required");
        try (ParticleDB db = new ParticleDB(Config.builder().host("127.0.0.1").port(Integer.parseInt(port)).build())) {
            String name = "java_pipeline_" + ProcessHandle.current().pid();
            db.rag().createPipeline(name, "documents", "embedding", "content", 5, "vector");
            db.execute("DROP RAG PIPELINE " + name);
            assertThrows(IllegalArgumentException.class, () -> db.rag().createPipeline("bad; SELECT 1", "documents", "embedding", "content", 5, "vector"));
            assertThrows(IllegalArgumentException.class, () -> db.rag().createPipeline(name, "documents", "embedding", "content", 5, "rerank"));
        }
    }
}
