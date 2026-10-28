-- Sinex: report editor events to `sinex-session`'s integration socket.
--
--   require("sinex").setup()
--
-- Each event is one JSON line on the socket ($SINEX_SESSION_SOCKET, or
-- $XDG_RUNTIME_DIR/sinex/session.sock). When nothing listens, events are
-- dropped silently: the editor never waits on Sinex.
local M = {}

local events = {
  "VimEnter", "VimLeavePre", "BufReadPost", "BufNewFile", "BufEnter",
  "BufWritePost", "BufDelete", "DirChanged", "FocusGained", "FocusLost", "TermOpen",
}

local function socket_path()
  local p = vim.env.SINEX_SESSION_SOCKET
  if p and p ~= "" then return p end
  local run = vim.env.XDG_RUNTIME_DIR
  if not run or run == "" then return nil end
  return run .. "/sinex/session.sock"
end

local chan = nil

local function send(line)
  local path = socket_path()
  if not path then return end
  if not chan then
    local ok, c = pcall(vim.fn.sockconnect, "pipe", path, vim.empty_dict())
    if not ok or c == 0 then return end
    chan = c
  end
  if not pcall(vim.fn.chansend, chan, line .. "\n") then
    -- The listener went away; reconnect on the next event.
    pcall(vim.fn.chanclose, chan)
    chan = nil
  end
end

local function git_identity(cwd)
  local function git(args)
    local cmd = { "git", "--no-optional-locks", "-C", cwd }
    vim.list_extend(cmd, args)
    local ok, result = pcall(function() return vim.system(cmd, { text = true }):wait(200) end)
    if not ok or result.code ~= 0 then return nil end
    return vim.trim(result.stdout or "")
  end
  local common = git({ "rev-parse", "--path-format=absolute", "--git-common-dir" })
  local roots = git({ "rev-list", "--max-parents=0", "HEAD" })
  if not common or not roots then return vim.NIL end
  local commits = vim.split(roots, "\n", { trimempty = true })
  table.sort(commits)
  return { common_dir = common, root_commits = commits,
    head = git({ "rev-parse", "HEAD" }), branch = git({ "symbolic-ref", "--quiet", "--short", "HEAD" }) }
end

function M.setup(opts)
  opts = opts or {}
  local group = vim.api.nvim_create_augroup("sinex", { clear = true })
  vim.api.nvim_create_autocmd(opts.events or events, {
    group = group,
    callback = function(ev)
      local name = vim.api.nvim_buf_get_name(ev.buf)
      send(vim.json.encode({
        source = "neovim",
        event = ev.event,
        path = name ~= "" and name or nil,
        filetype = vim.bo[ev.buf].filetype ~= "" and vim.bo[ev.buf].filetype or nil,
        cwd = vim.fn.getcwd(),
        git_identity = git_identity(vim.fn.getcwd()),
        pid = vim.fn.getpid(),
        buf = ev.buf,
      }))
    end,
  })
end

return M
