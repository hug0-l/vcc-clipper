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
        ('AGENTS.md', '.'),
        ('CHANGELOG.md', '.'),
        ('README.md', '.'),
        ('protocol.md', '.'),
        ('js', 'js'),
        ('services', 'services'),
    ],
    hiddenimports=['websockets', 'asyncio'],
    hookspath=[],
    runtime_hooks=[],
    excludes=[
        'tkinter', 'test', 'unittest', 'email', 'http', 'urllib',
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
