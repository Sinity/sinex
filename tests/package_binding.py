"""The narrow source-delta guard for reusing an already qualified package."""

def require_harness_only(paths):
    assert all(p.startswith(('tests/', 'docs/')) or p in ('README.md', '.agentctl/project.toml')
        for p in paths), 'qualification requires a package with unchanged runtime/build source'
