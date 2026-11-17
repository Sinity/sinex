/* A test-only observation inside the datatype input call's memory lifetime. */
#include <postgres.h>
#include <fmgr.h>
#include <varatt.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(receive_memory);
Datum
receive_memory(PG_FUNCTION_ARGS)
{
    Oid input, parameter;
    char *wire = text_to_cstring(PG_GETARG_TEXT_PP(1));
    MemoryContext parent, receive;
    Size allocated;
    Datum value;
    getTypeInputInfo(PG_GETARG_OID(0), &input, &parameter);
    receive = AllocSetContextCreate(CurrentMemoryContext, "receive memory control",
                                   ALLOCSET_DEFAULT_SIZES);
    parent = MemoryContextSwitchTo(receive);
    value = OidInputFunctionCall(input, wire, parameter, -1);
    /* The value is a real compressed datum; input temporaries remain visible. */
    if (VARSIZE_ANY(DatumGetPointer(value)) == 0)
        elog(ERROR, "empty compressed datum");
    allocated = MemoryContextMemAllocated(receive, true);
    MemoryContextSwitchTo(parent);
    MemoryContextDelete(receive);
    PG_RETURN_INT64((int64) allocated);
}
