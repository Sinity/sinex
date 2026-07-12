{ lib }:

let
  mkSpec =
    optionName: surfaceName: automaton: description:
    {
      inherit optionName surfaceName automaton description;
    };
in
rec {
  # Named automaton activation profiles (sinex-ijz6, ratified rulings pq5/vfy/
  # nbi.4). A catalog entry may declare `activationProfile = "<name>"`, which
  # additionally requires "<name>" to appear in
  # `services.sinex.automata.enabledProfiles` before the automaton is
  # eligible to run, on top of its own per-automaton `enable` flag. This is
  # a DIFFERENT axis than the per-automaton `profile` option (performance
  # tier: light/standard/heavy) — activation profiles gate WHETHER an
  # automaton may run at all, not how it is resourced.
  activationProfiles = [ "document" "actuation" ];

  specs = [
    (mkSpec "canonicalizer" "canonicalizer" "canonicalizer" "Sinex canonical command synthesizer")
    (mkSpec "healthAggregator" "health_aggregator" "health" "Sinex health automaton")
    (mkSpec "analyticsAutomaton" "analytics_automaton" "analytics" "Sinex analytics automaton")
    (mkSpec "attentionStream" "attention_stream" "attention-stream" "Sinex attention-stream automaton")
    (mkSpec "intervalLift" "interval_lift" "interval-lift" "Sinex interval-lift automaton")
    (mkSpec "sessionDetector" "session_detector" "session" "Sinex session detector")
    (mkSpec "hourlySummarizer" "hourly_summarizer" "hourly" "Sinex hourly activity summarizer")
    (mkSpec "dailySummarizer" "daily_summarizer" "daily" "Sinex daily activity summarizer")
    ((mkSpec "documentParser" "document_parser" "document-parser" "Sinex document parser automaton")
      // { activationProfile = "document"; })
    (mkSpec "embeddingProducer" "embedding_producer" "embedding-producer" "Sinex document chunk embedding producer")
    (mkSpec "tagApplier" "tag_applier" "tag-applier" "Sinex rule-based tag applier automaton")
    ((mkSpec "instructionReconciler" "instruction_reconciler" "instruction-reconciler" "Sinex instruction expectation reconciler")
      // { activationProfile = "actuation"; })
    (mkSpec "entityExtractor" "entity_extractor" "entity-extractor" "Sinex entity extractor automaton")
    (mkSpec "entityResolver" "entity_resolver" "entity-resolver" "Sinex entity resolver automaton")
    (mkSpec "relationExtractor" "relation_extractor" "relation-extractor" "Sinex relation extractor automaton")
    (mkSpec "entityEnricher" "entity_enricher" "entity-enricher" "Sinex entity enricher automaton")
  ];

  # True when `spec` is eligible to run given the host's enabled activation
  # profile set: automata with no declared `activationProfile` are always
  # eligible (ungated by this mechanism); gated automata require their
  # profile name to be a member of `enabledProfiles`.
  profileAllows = enabledProfiles: spec:
    let profile = spec.activationProfile or null;
    in profile == null || lib.elem profile enabledProfiles;

  countEnabled = automataCfg:
    lib.length (lib.filter
      (spec: automataCfg.${spec.optionName}.enable && profileAllows automataCfg.enabledProfiles spec)
      specs);
}
