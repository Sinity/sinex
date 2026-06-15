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
    && (raw.schema_version or null) == 1
    && isList (raw.entries or null);

  entries =
    if requiredTopLevel then raw.entries
    else throw "source-catalog.generated.json must be an object with schema_version = 1 and entries = [ ... ]";

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

  hasRequiredLimits = entry:
    let
      binding = entry.binding or null;
      limits = entry.resource_limits or null;
    in
    binding == null || (
      isAttrs limits
      && isInt (limits.memory_max_mib or null)
      && limits.memory_max_mib > 0
      && isInt (limits.cpu_weight or null)
      && limits.cpu_weight >= 1
      && limits.cpu_weight <= 10000
    );

  invalidEntries =
    filter
      (entry: !(hasRequiredContract entry && hasRequiredBinding entry && hasRequiredLimits entry))
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

  resourceLimitsFor = sourceId: (boundEntryFor sourceId).resource_limits;

  shutdownTimeoutFor = sourceId:
    let shape = runtimeShapeFor sourceId;
    in if shape == "scheduled" || shape == "on_demand" then 600 else 90;

  openFilesLimitFor = sourceId:
    let
      entry = boundEntryFor sourceId;
      scope = entry.contract.access_scope.scope;
    in
    if sourceId == "fs" || scope == "configured_roots" then 524288 else null;

  resourceDefaultsFor = sourceId:
    let
      limits = resourceLimitsFor sourceId;
      memory = "${toString limits.memory_max_mib}M";
      openFilesLimit = openFilesLimitFor sourceId;
    in
    {
      memoryHigh = memory;
      memoryMax = memory;
      cpuQuota = null;
      cpuWeight = limits.cpu_weight;
      ioWeight = 10;
      ioSchedulingClass = "idle";
      nice = 10;
      shutdownTimeoutSec = shutdownTimeoutFor sourceId;
      inherit openFilesLimit;
    };

  instanceDefaultFor = sourceId:
    let shape = runtimeShapeFor sourceId;
    in
    if shape == "continuous" || shape == "scheduled" || shape == "on_demand" then 1
    else 1;

  manifestMetadataFor = sourceId:
    let
      entry = boundEntryFor sourceId;
      limits = entry.resource_limits;
    in
    {
      runner_pack = entry.binding.runner_pack;
      runtime_shape = entry.binding.runtime_shape;
      access_scope = entry.contract.access_scope;
      privacy_tier = entry.contract.privacy_tier;
      resource_limits = limits;
    };

  memoryFor = sourceId: (resourceLimitsFor sourceId).memory_max_mib;

  aggregateMemoryFor = sourceIds:
    foldl' (total: sourceId: total + memoryFor sourceId) 0 sourceIds;

  unitMemoryLimitFor = sourceIds:
    let total = aggregateMemoryFor sourceIds;
    in
    if sourceIds == [ ] then { }
    else { MemoryMax = "${toString total}M"; };

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
    resourceDefaultsFor
    resourceLimitsFor
    unitMemoryLimitFor
    ;

  requireFieldsFor = sourceIds:
    let missing = filter (sourceId: !(hasAttr sourceId byId)) sourceIds;
    in
    if missing != [ ] then
      throw "source catalog is missing required source ids: ${concatStringsSep ", " missing}"
    else genAttrs sourceIds (sourceId: manifestMetadataFor sourceId);
}
