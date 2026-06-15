{ lib }:

let
  mkSpec =
    optionName: surfaceName: automaton: description:
    {
      inherit optionName surfaceName automaton description;
    };
in
rec {
  specs = [
    (mkSpec "canonicalizer" "canonicalizer" "canonicalizer" "Sinex canonical command synthesizer")
    (mkSpec "healthAggregator" "health_aggregator" "health" "Sinex health automaton")
    (mkSpec "analyticsAutomaton" "analytics_automaton" "analytics" "Sinex analytics automaton")
    (mkSpec "sessionDetector" "session_detector" "session" "Sinex session detector")
    (mkSpec "hourlySummarizer" "hourly_summarizer" "hourly" "Sinex hourly activity summarizer")
    (mkSpec "dailySummarizer" "daily_summarizer" "daily" "Sinex daily activity summarizer")
    (mkSpec "documentParser" "document_parser" "document-parser" "Sinex document parser automaton")
    (mkSpec "embeddingProducer" "embedding_producer" "embedding-producer" "Sinex document chunk embedding producer")
    (mkSpec "tagApplier" "tag_applier" "tag-applier" "Sinex rule-based tag applier automaton")
    (mkSpec "instructionReconciler" "instruction_reconciler" "instruction-reconciler" "Sinex instruction expectation reconciler")
    (mkSpec "entityExtractor" "entity_extractor" "entity-extractor" "Sinex entity extractor automaton")
    (mkSpec "entityResolver" "entity_resolver" "entity-resolver" "Sinex entity resolver automaton")
    (mkSpec "relationExtractor" "relation_extractor" "relation-extractor" "Sinex relation extractor automaton")
    (mkSpec "entityEnricher" "entity_enricher" "entity-enricher" "Sinex entity enricher automaton")
  ];

  countEnabled = automataCfg:
    lib.length (lib.filter (spec: automataCfg.${spec.optionName}.enable) specs);
}
