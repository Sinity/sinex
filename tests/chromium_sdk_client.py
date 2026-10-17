#!/usr/bin/env python3
"""Recorded public Sinex calls through the official Python MCP SDK.

The surrounding generated fixture owns expected facts. The recording seam is
adapted from the joint consumer at c960ce13a42c3daa5f0d17d6dabeb35971cfb0b1217b19bced77a8158365a9c6.
"""
import argparse
import asyncio
from importlib.metadata import version
import json
import os
from pathlib import Path
import traceback
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

class RecordedSession(ClientSession):
    def __init__(self, read, write, record):
        super().__init__(read, write)
        self.record = record

    async def send_request(self, request, result_type, *args, **kwargs):
        entry = {'request': request.model_dump(mode='json', by_alias=True, exclude_none=True)}
        self.record['sdk_messages'].append(entry)
        try:
            result = await super().send_request(request, result_type, *args, **kwargs)
            entry['response'] = result.model_dump(mode='json', by_alias=True, exclude_none=True)
            return result
        except BaseException as error:
            entry['error'] = repr(error)
            raise

    async def send_notification(self, notification, *args, **kwargs):
        self.record['sdk_messages'].append({'notification': notification.model_dump(mode='json', by_alias=True, exclude_none=True)})
        return await super().send_notification(notification, *args, **kwargs)


async def investigate(args, record):
    server = StdioServerParameters(command=args.binary,args=['--endpoint',args.endpoint,'--token-file',args.token_file,'mcp'],env=dict(os.environ))
    with (args.output/'server.stderr').open('w') as errlog:
        async with stdio_client(server,errlog=errlog) as (read,write):
            async with RecordedSession(read,write,record) as session:
                initialized=await session.initialize()
                record['negotiated_version']=initialized.protocolVersion
                listing=await session.list_tools()
                record['tools']=listing.model_dump(mode='json',by_alias=True,exclude_none=True)
                assert {'sinex_query','sinex_show','sinex_trace'} <= {t.name for t in listing.tools}
                async def tool(name,arguments):
                    result=await session.call_tool(name,arguments)
                    assert not result.isError,result
                    assert result.structuredContent is not None,'tool omitted structuredContent'
                    return result.structuredContent
                answer=await tool('sinex_query',{'query':args.query})
                record['query']=answer
                assert answer['rows'],'public discovery returned no rows'
                # Discovery supplies the reference; no event ID enters this client.
                reference='sinex:ev/'+answer['rows'][0]['id']
                record['reference']=reference
                record['show']=await tool('sinex_show',{'ref':reference})
                record['trace']=await tool('sinex_trace',{'ref':reference})


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('binary','endpoint','token-file','query'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    record={'success':False,'sdk':{'name':'mcp','version':version('mcp')},'sdk_messages':[],
            'server_argv':[args.binary,'--endpoint',args.endpoint,'--token-file',args.token_file,'mcp'],
            'process_exit_status':'not exposed by official SDK stdio context; context closure is recorded'}
    try:
        asyncio.run(asyncio.wait_for(investigate(args,record),timeout=90))
        record['sdk_context_closed']=True;record['success']=True
    except BaseException as error:
        record['failure']=repr(error)
        (args.output/'failure.txt').write_text(traceback.format_exc())
    finally:
        (args.output/'result.json').write_text(json.dumps(record,indent=2)+'\n')
        print(json.dumps({'success':record['success'],'output':str(args.output)}))
    return 0 if record['success'] else 1

if __name__=='__main__':raise SystemExit(main())
