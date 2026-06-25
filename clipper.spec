# -*- mode: python ; coding: utf-8 -*-
import os, sys

block_cipher = None

a = Analysis(
    ['signal_server.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('clipper.html', '.'),
        ('clipper-sdk.js', '.'),
        ('PLUGINS.md', '.'),
        ('AGENTS.md', '.'),
        ('CHANGELOG.md', '.'),
        ('README.md', '.'),
        ('protocol.md', '.'),
        ('manifest.json', '.'),
        ('sw.js', '.'),
        ('js', 'js'),
        ('services', 'services'),
        ('plugins', 'plugins'),
        ('server_plugins', 'server_plugins'),
    ],
    hiddenimports=[
        'websockets',
        'websockets.legacy',
        'websockets.legacy.server',
        'asyncio',
        'sqlite3',
        'hashlib',
        'hmac',
        'services',
        'services.ws_router',
        'services.persistence',
        'services.chat_service',
        'services.notice_service',
        'services.room_service',
        'services.keymgmt_service',
        'services.checklist_service',
        'services.plugin_loader',
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[
        'tkinter', 'test', 'unittest', 'email',
        'pydoc', 'doctest', 'distutils', 'setuptools', 'pip',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='clipper-server',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

app = BUNDLE(
    exe,
    a.zipfiles,
    a.datas,
    [],
    name='Clipper.app',
    icon=None,
    bundle_identifier='com.clipper.server',
    info_plist={
        'NSHighResolutionCapable': 'True',
        'CFBundleDisplayName': 'Clipper Server',
        'CFBundleName': 'Clipper',
        'CFBundleVersion': '2.0.0',
        'CFBundleShortVersionString': '2.0.0',
    },
)
