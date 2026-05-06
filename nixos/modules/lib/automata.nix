{ lib }:

let
  mkSpec =
    optionName: surfaceName: serviceName: automaton: binary: description:
    {
      inherit optionName surfaceName serviceName automaton binary description;
    };
in
rec {
  specs = [
    (mkSpec "canonicalizer" "canonicalizer" "sinex-canonicalizer" "canonicalizer" "terminal-command-canonicalizer" "Sinex canonical command synthesizer")
    (mkSpec "healthAggregator" "health_aggregator" "sinex-health-automaton" "health" "health-automaton" "Sinex health automaton")
    (mkSpec "analyticsAutomaton" "analytics_automaton" "sinex-analytics-automaton" "analytics" "analytics-automaton" "Sinex analytics automaton")
    (mkSpec "sessionDetector" "session_detector" "sinex-session-detector" "session" "session-detector" "Sinex session detector")
    (mkSpec "hourlySummarizer" "hourly_summarizer" "sinex-hourly-summarizer" "hourly" "hourly-summarizer" "Sinex hourly activity summarizer")
    (mkSpec "dailySummarizer" "daily_summarizer" "sinex-daily-summarizer" "daily" "daily-summarizer" "Sinex daily activity summarizer")
    (mkSpec "documentParser" "document_parser" "sinex-document-parser" "document-parser" "document-parser" "Sinex document parser automaton")
    (mkSpec "tagApplier" "tag_applier" "sinex-tag-applier" "tag-applier" "tag-applier" "Sinex rule-based tag applier automaton")
    (mkSpec "entityExtractor" "entity_extractor" "sinex-entity-extractor" "entity-extractor" "entity-extractor" "Sinex entity extractor automaton")
    (mkSpec "entityResolver" "entity_resolver" "sinex-entity-resolver" "entity-resolver" "entity-resolver" "Sinex entity resolver automaton")
    (mkSpec "relationExtractor" "relation_extractor" "sinex-relation-extractor" "relation-extractor" "relation-extractor" "Sinex relation extractor automaton")
    (mkSpec "entityEnricher" "entity_enricher" "sinex-entity-enricher" "entity-enricher" "entity-enricher" "Sinex entity enricher automaton")
  ];

  countEnabled = automataCfg:
    lib.length (lib.filter (spec: automataCfg.${spec.optionName}.enable) specs);
}
