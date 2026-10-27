"""Generate a synthetic Takeout member with complete or withdrawn video titles."""
import datetime
import pathlib
import sys
import zipfile

path, count, changed = pathlib.Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == 'changed'
with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_STORED) as archive:
    with archive.open('Takeout/YouTube and YouTube Music/history/watch-history.html', 'w', force_zip64=True) as member:
        for index in range(count):
            url = f'https://www.youtube.com/watch?v=synthetic{index:016x}'
            title = url if changed else f'Synthetic video {index}'
            channel = '' if changed else '<br><a href="https://www.youtube.com/channel/synthetic">Synthetic channel</a>'
            at = datetime.datetime(2020, 1, 1) + datetime.timedelta(seconds=index)
            time = at.strftime('%b %d, %Y, %I:%M:%S %p UTC')
            text = (f'<div class="outer-cell"><div class="mdl-grid"><div class="header-cell">'
                    f'<p class="mdl-typography--title">YouTube<br></p></div>'
                    f'<div class="content-cell mdl-typography--body-1">Watched <a href="{url}">{title}</a>{channel}<br>{time}<br></div>'
                    f'<div class="content-cell mdl-typography--caption"><b>Products:</b><br>YouTube<br></div></div></div>')
            member.write(text.encode())
