package com.particledb;

import java.util.List;

/**
 * AI function operations for ParticleDB.
 *
 * Provides methods for embedding generation, text generation,
 * classification, and summarization using ParticleDB's built-in AI functions.
 *
 * <pre>{@code
 * // Generate an embedding
 * Row result = db.ai().embed("Hello world", null);
 *
 * // Generate text
 * String response = db.ai().generate("Explain vector databases");
 *
 * // Classify text
 * String label = db.ai().classify("Great product!",
 *     List.of("positive", "negative", "neutral"));
 * }</pre>
 */
public class AIOps {

    private final ParticleDB db;

    AIOps(ParticleDB db) {
        this.db = db;
    }

    /**
     * Generate a vector embedding for a text string.
     *
     * @param text  the text to embed
     * @param model optional model name (null for default)
     * @return the row containing the embedding
     */
    public Row embed(String text, String model) {
        if (model != null && !model.isEmpty()) {
            return db.queryOne("SELECT AI_EMBED(?, ?) AS embedding", text, model);
        }
        return db.queryOne("SELECT AI_EMBED(?) AS embedding", text);
    }

    /**
     * Generate text from a prompt.
     *
     * @param prompt the input prompt
     * @return the generated text response
     */
    public String generate(String prompt) {
        Row row = db.queryOne("SELECT AI_GENERATE(?) AS text", prompt);
        if (row == null) {
            throw new ParticleDBException("AI_GENERATE returned no result");
        }
        return row.getString("text");
    }

    /**
     * Classify text into one of the provided labels.
     *
     * @param text   the text to classify
     * @param labels list of possible classification labels
     * @return the predicted label
     */
    public String classify(String text, List<String> labels) {
        Row row = db.queryOne("SELECT AI_CLASSIFY(?, ?) AS label", text, SqlBuilder.labelsJson(labels));
        if (row == null) {
            throw new ParticleDBException("AI_CLASSIFY returned no result");
        }
        return row.getString("label");
    }

    /**
     * Summarize a block of text.
     *
     * @param text the text to summarize
     * @return a summary of the input text
     */
    public String summarize(String text) {
        Row row = db.queryOne("SELECT AI_SUMMARIZE(?) AS summary", text);
        if (row == null) {
            throw new ParticleDBException("AI_SUMMARIZE returned no result");
        }
        return row.getString("summary");
    }
}
