{ lib }:

let
  inherit (lib)
    concatStringsSep
    filter
    filterAttrs
    foldl'
    genAttrs
    hasAttr
    isAttrs
    isInt
    isList
    isString
    listToAttrs
    mapAttrsToList
    nameValuePair
    ;

  raw = builtins.fromJSON (builtins.readFile ../source-catalog.generated.json);

  requiredTopLevel =
    (isAttrs raw)
    && (raw.schema_version or null) == 3
    && isList (raw.entries or null);

  entries =
    if requiredTopLevel then raw.entries
    else throw "source-catalog.generated.json must be an object with schema_version = 3 and entries = [ ... ]";

  hasRequiredContract = entry:
    let contract = entry.contract or null;
    in
    isAttrs contract
    && isString (contract.id or null)
    && contract.id != ""
    && isString (contract.privacy_tier or null)
    && isAttrs (contract.access_scope or null)
    && isString (contract.access_scope.scope or null);

  hasRequiredBinding = entry:
    let binding = entry.binding or null;
    in
    binding == null || (
      isAttrs binding
      && isString (binding.source_id or null)
      && binding.source_id != ""
      && isString (binding.runner_pack or null)
      && isString (binding.runtime_shape or null)
    );

  isOptionalInt = value: value == null || isInt value;

  hasRequiredBudget = entry:
    let
      binding = entry.binding or null;
      budget = entry.work_budget or null;
    in
    binding == null || (
      isAttrs budget
      && isString (budget.work_class or null)
      && isInt (budget.steady_memory_mib or null)
      && budget.steady_memory_mib > 0
      && isInt (budget.burst_memory_mib or null)
      && budget.burst_memory_mib >= budget.steady_memory_mib
      && isInt (budget.cpu_weight or null)
      && budget.cpu_weight >= 1
      && budget.cpu_weight <= 10000
      && isOptionalInt (budget.max_input_bytes_per_sec or null)
      && isOptionalInt (budget.max_input_events_per_sec or null)
      && isInt (budget.max_pending_material_bytes or null)
      && budget.max_pending_material_bytes >= 0
      && isInt (budget.max_pending_candidates or null)
      && budget.max_pending_candidates >= 0
      && isOptionalInt (budget.max_unacked_transport_messages or null)
      && isOptionalInt (budget.batch_size or null)
      && isOptionalInt (budget.flush_interval_ms or null)
      && isOptionalInt (budget.checkpoint_interval_ms or null)
      && isOptionalInt (budget.expected_disk_write_bytes_per_min or null)
      && isOptionalInt (budget.expected_wal_write_bytes_per_min or null)
      && isList (budget.pressure_actions or null)
    );

  invalidEntries =
    filter
      (entry: !(hasRequiredContract entry && hasRequiredBinding entry && hasRequiredBudget entry))
      entries;

  duplicateIds =
    let
      ids = map (entry: entry.contract.id) entries;
      counts = foldl' (acc: id: acc // { ${id} = (acc.${id} or 0) + 1; }) { } ids;
    in
    mapAttrsToList (id: _: id) (filterAttrs (_: count: count > 1) counts);

  validation =
    if invalidEntries != [ ] then
      throw "source-catalog.generated.json has entries missing required contract/binding/resource fields"
    else if duplicateIds != [ ] then
      throw "source-catalog.generated.json has duplicate contract ids: ${concatStringsSep ", " duplicateIds}"
    else {
      schemaVersion = raw.schema_version;
      entryCount = builtins.length entries;
    };

  byId = listToAttrs (
    map (entry: nameValuePair entry.contract.id entry) entries
  );

  entryFor = sourceId:
    byId.${sourceId} or (throw "source catalog has no entry for source id '${sourceId}'");

  boundEntryFor = sourceId:
    let entry = entryFor sourceId;
    in
    if entry.binding or null == null then
      throw "source catalog entry '${sourceId}' has no runtime binding"
    else entry;

  runtimeShapeFor = sourceId: (boundEntryFor sourceId).binding.runtime_shape;

  workBudgetFor = sourceId: (boundEntryFor sourceId).work_budget;

  shutdownTimeoutFor = sourceId:
    let shape = runtimeShapeFor sourceId;
    in if shape == "scheduled" || shape == "on_demand" then 600 else 90;

  startupTimeoutFor = _sourceId: 600;

  openFilesLimitFor = sourceId:
    let
      entry = boundEntryFor sourceId;
      scope = entry.contract.access_scope.scope;
    in
    if sourceId == "fs" || scope == "configured_roots" then 524288 else null;

  instanceDefaultFor = sourceId:
    let shape = runtimeShapeFor sourceId;
    in
    if shape == "continuous" || shape == "scheduled" || shape == "on_demand" then 1
    else 1;

  manifestMetadataFor = sourceId:
    let
      entry = boundEntryFor sourceId;
    in
    {
      runner_pack = entry.binding.runner_pack;
      runtime_shape = entry.binding.runtime_shape;
      access_scope = entry.contract.access_scope;
      privacy_tier = entry.contract.privacy_tier;
      work_budget = entry.work_budget;
    };

  allSourceIds = map (entry: entry.contract.id) entries;

in
validation // {
  inherit
    allSourceIds
    entries
    byId
    entryFor
    instanceDefaultFor
    manifestMetadataFor
    workBudgetFor
    ;

  requireFieldsFor = sourceIds:
    let missing = filter (sourceId: !(hasAttr sourceId byId)) sourceIds;
    in
    if missing != [ ] then
      throw "source catalog is missing required source ids: ${concatStringsSep ", " missing}"
    else genAttrs sourceIds (sourceId: manifestMetadataFor sourceId);
}
