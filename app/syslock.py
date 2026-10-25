"""
Full-screen break lock for Windows (used only when the app runs on the
colleague's own PC). While a break is on it:
  * covers every monitor with a topmost window and keeps it in front,
  * swallows Alt+Tab, Alt+Esc, Alt+F4, Ctrl+Esc, Ctrl+Shift+Esc and the Windows key
    (so Win+D, Win+Tab etc. do nothing) using a keyboard hook, and
  * greys out the Ctrl+Alt+Del options (Lock, Task Manager, Change password,
    Sign out) for the current user while the lock is on.
Ctrl+Alt+Del itself is handled by Windows and cannot be blocked by any
program; its screen still appears, press Esc to return to the lock screen.
Everything is released when the colleague picks a break type and presses
Resume, when the server stops, or after a 4-hour failsafe.
Nothing is installed and no administrator rights are needed.
"""
import atexit
import json
import os
import sys
import tempfile
import threading
import time

IS_WIN = sys.platform.startswith("win")
BREAK_TYPES = ["Tea Break", "Lunch Break", "Other Break", "Meeting Break"]
FAILSAFE_SECONDS = 4 * 3600
_MARKER = os.path.join(tempfile.gettempdir(), "wfh_syslock_registry.json")

state = {"active": False, "resumed": False, "chosen": "", "start": 0.0}
_lock = threading.Lock()
_hwnd_box = {"hwnd": None}

# (hive-relative key, value name) pairs set to 1 while locked
_POLICIES = [
    (r"Software\Microsoft\Windows\CurrentVersion\Policies\System", "DisableTaskMgr"),
    (r"Software\Microsoft\Windows\CurrentVersion\Policies\System", "DisableLockWorkstation"),
    (r"Software\Microsoft\Windows\CurrentVersion\Policies\System", "DisableChangePassword"),
    (r"Software\Microsoft\Windows\CurrentVersion\Policies\Explorer", "NoLogoff"),
]


def _policies_apply():
    if not IS_WIN:
        return
    import winreg
    saved = []
    for key, name in _POLICIES:
        try:
            k = winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, key, 0, winreg.KEY_READ | winreg.KEY_SET_VALUE)
            try:
                old = winreg.QueryValueEx(k, name)[0]
            except OSError:
                old = None
            saved.append([key, name, old])
            winreg.SetValueEx(k, name, 0, winreg.REG_DWORD, 1)
            winreg.CloseKey(k)
        except OSError:
            pass
    try:
        with open(_MARKER, "w") as f:
            json.dump(saved, f)
    except OSError:
        pass


def policies_restore():
    """Put back whatever the registry held before the lock (also run at server start)."""
    if not IS_WIN or not os.path.exists(_MARKER):
        return
    import winreg
    try:
        with open(_MARKER) as f:
            saved = json.load(f)
    except (OSError, ValueError):
        saved = []
    for key, name, old in saved:
        try:
            k = winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, key, 0, winreg.KEY_SET_VALUE)
            if old is None:
                try:
                    winreg.DeleteValue(k, name)
                except OSError:
                    pass
            else:
                winreg.SetValueEx(k, name, 0, winreg.REG_DWORD, int(old))
            winreg.CloseKey(k)
        except OSError:
            pass
    try:
        os.remove(_MARKER)
    except OSError:
        pass


def _run_window():
    import ctypes
    from ctypes import wintypes
    user32, gdi32, kernel32 = ctypes.windll.user32, ctypes.windll.gdi32, ctypes.windll.kernel32
    LRESULT, WPARAM, LPARAM = ctypes.c_ssize_t, ctypes.c_size_t, ctypes.c_ssize_t
    HWND = wintypes.HWND
    WNDPROC = ctypes.WINFUNCTYPE(LRESULT, HWND, wintypes.UINT, WPARAM, LPARAM)
    HOOKPROC = ctypes.WINFUNCTYPE(LRESULT, ctypes.c_int, WPARAM, LPARAM)

    user32.DefWindowProcW.argtypes = [HWND, wintypes.UINT, WPARAM, LPARAM]
    user32.DefWindowProcW.restype = LRESULT
    user32.CallNextHookEx.argtypes = [wintypes.HANDLE, ctypes.c_int, WPARAM, LPARAM]
    user32.CallNextHookEx.restype = LRESULT
    user32.SetWindowsHookExW.argtypes = [ctypes.c_int, HOOKPROC, wintypes.HINSTANCE, wintypes.DWORD]
    user32.SetWindowsHookExW.restype = wintypes.HANDLE
    user32.SendMessageW.argtypes = [HWND, wintypes.UINT, WPARAM, LPARAM]
    user32.SendMessageW.restype = LRESULT
    user32.PostMessageW.argtypes = [HWND, wintypes.UINT, WPARAM, LPARAM]
    user32.CreateWindowExW.restype = HWND
    user32.CreateWindowExW.argtypes = [wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD,
                                       ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                       HWND, wintypes.HMENU, wintypes.HINSTANCE, wintypes.LPVOID]
    kernel32.GetModuleHandleW.restype = wintypes.HINSTANCE
    user32.SetWindowPos.argtypes = [HWND, HWND, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, wintypes.UINT]
    user32.GetForegroundWindow.restype = HWND
    user32.SetForegroundWindow.argtypes = [HWND]
    user32.DestroyWindow.argtypes = [HWND]
    user32.SetWindowTextW.argtypes = [HWND, wintypes.LPCWSTR]
    user32.EnableWindow.argtypes = [HWND, wintypes.BOOL]
    user32.SetTimer.argtypes = [HWND, ctypes.c_size_t, wintypes.UINT, wintypes.LPVOID]
    user32.LoadCursorW.restype = wintypes.HANDLE
    user32.LoadCursorW.argtypes = [wintypes.HINSTANCE, ctypes.c_void_p]
    user32.GetAsyncKeyState.argtypes = [ctypes.c_int]
    user32.UnhookWindowsHookEx.argtypes = [wintypes.HANDLE]
    user32.GetMessageW.argtypes = [ctypes.POINTER(wintypes.MSG), HWND, wintypes.UINT, wintypes.UINT]
    gdi32.CreateSolidBrush.restype = wintypes.HANDLE
    gdi32.CreateFontW.restype = wintypes.HANDLE
    gdi32.SetTextColor.argtypes = [wintypes.HDC, wintypes.COLORREF]
    gdi32.SetBkMode.argtypes = [wintypes.HDC, ctypes.c_int]

    class KBD(ctypes.Structure):
        _fields_ = [("vkCode", wintypes.DWORD), ("scanCode", wintypes.DWORD), ("flags", wintypes.DWORD),
                    ("time", wintypes.DWORD), ("dwExtraInfo", ctypes.c_size_t)]

    class WNDCLASS(ctypes.Structure):
        _fields_ = [("style", wintypes.UINT), ("lpfnWndProc", WNDPROC), ("cbClsExtra", ctypes.c_int),
                    ("cbWndExtra", ctypes.c_int), ("hInstance", wintypes.HINSTANCE), ("hIcon", wintypes.HANDLE),
                    ("hCursor", wintypes.HANDLE), ("hbrBackground", wintypes.HANDLE),
                    ("lpszMenuName", wintypes.LPCWSTR), ("lpszClassName", wintypes.LPCWSTR)]

    WM_DESTROY, WM_CLOSE, WM_COMMAND, WM_TIMER, WM_CTLCOLORSTATIC, WM_SETFONT = 0x2, 0x10, 0x111, 0x113, 0x138, 0x30
    WS_POPUP, WS_VISIBLE, WS_CHILD, BS_PUSHBUTTON, SS_CENTER = 0x80000000, 0x10000000, 0x40000000, 0, 1
    WS_EX_TOPMOST, WS_EX_TOOLWINDOW = 0x8, 0x80
    HWND_TOPMOST, SWP_SHOWWINDOW = HWND(-1), 0x40
    VK_TAB, VK_ESCAPE, VK_F4, VK_LWIN, VK_RWIN, VK_CONTROL, VK_APPS = 0x09, 0x1B, 0x73, 0x5B, 0x5C, 0x11, 0x5D

    bg = gdi32.CreateSolidBrush(51 << 16 | 26 << 8 | 8)

    def font(h, weight=700):
        return gdi32.CreateFontW(-h, 0, 0, 0, weight, 0, 0, 0, 1, 0, 0, 5, 0, "Segoe UI")
    f_big, f_mid, f_btn = font(44), font(34), font(22)

    vx, vy = user32.GetSystemMetrics(76), user32.GetSystemMetrics(77)
    vw, vh = user32.GetSystemMetrics(78), user32.GetSystemMetrics(79)
    sw, sh = user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)
    cx, top = -vx + sw // 2, -vy + max(40, sh // 2 - 260)

    ctl = {}
    chosen = {"v": ""}

    def refresh_buttons():
        for i, t in enumerate(BREAK_TYPES):
            user32.SetWindowTextW(ctl["b%d" % i], ("✔  " if chosen["v"] == t else "") + t)
        user32.EnableWindow(ctl["resume"], bool(chosen["v"]))

    def tick_text():
        s = int(time.time() - state["start"])
        txt = "%02d:%02d:%02d" % (s // 3600, s % 3600 // 60, s % 60) if s >= 3600 else "%02d:%02d" % (s // 60, s % 60)
        user32.SetWindowTextW(ctl["timer"], txt)

    def wndproc(hwnd, msg, wp, lp):
        if msg == WM_COMMAND:
            cid = wp & 0xFFFF
            if 101 <= cid <= 104:
                chosen["v"] = BREAK_TYPES[cid - 101]
                refresh_buttons()
                return 0
            if cid == 105 and chosen["v"]:
                with _lock:
                    state["chosen"], state["resumed"] = chosen["v"], True
                user32.DestroyWindow(hwnd)
                return 0
        elif msg == WM_TIMER:
            tick_text()
            if time.time() - state["start"] > FAILSAFE_SECONDS:
                user32.DestroyWindow(hwnd)
                return 0
            user32.SetWindowPos(hwnd, HWND_TOPMOST, vx, vy, vw, vh, SWP_SHOWWINDOW)
            if user32.GetForegroundWindow() != hwnd:
                user32.SetForegroundWindow(hwnd)
            return 0
        elif msg == WM_CTLCOLORSTATIC:
            gdi32.SetTextColor(wp, 0xFFFFFF)
            gdi32.SetBkMode(wp, 1)
            return bg
        elif msg == WM_CLOSE:
            return 0          # ignore Alt+F4 / close requests; only Resume (or the failsafe) ends the lock
        elif msg == WM_DESTROY:
            user32.PostQuitMessage(0)
            return 0
        return user32.DefWindowProcW(hwnd, msg, wp, lp)

    wnd_cb = WNDPROC(wndproc)
    hinst = kernel32.GetModuleHandleW(None)
    wc = WNDCLASS()
    wc.lpfnWndProc, wc.hInstance, wc.hbrBackground = wnd_cb, hinst, bg
    wc.hCursor = user32.LoadCursorW(None, 32512)
    wc.lpszClassName = "WFHBreakLock%d" % int(time.time())
    user32.RegisterClassW(ctypes.byref(wc))

    hwnd = user32.CreateWindowExW(WS_EX_TOPMOST | WS_EX_TOOLWINDOW, wc.lpszClassName, "Break lock",
                                  WS_POPUP | WS_VISIBLE, vx, vy, vw, vh, None, None, hinst, None)
    _hwnd_box["hwnd"] = hwnd

    def child(cls, text, style, x, y, w, h, cid, fnt):
        c = user32.CreateWindowExW(0, cls, text, WS_CHILD | WS_VISIBLE | style, x, y, w, h, hwnd, cid, hinst, None)
        user32.SendMessageW(c, WM_SETFONT, fnt, 1)
        return c
    ctl["title"] = child("STATIC", "\U0001F512  Screen locked", SS_CENTER, cx - 450, top, 900, 64, 0, f_big)
    ctl["sub"] = child("STATIC", "Select the reason for your break, then press Resume Work", SS_CENTER, cx - 450, top + 70, 900, 34, 0, f_btn)
    ctl["timer"] = child("STATIC", "00:00", SS_CENTER, cx - 200, top + 115, 400, 56, 0, f_mid)
    for i, t in enumerate(BREAK_TYPES):
        ctl["b%d" % i] = child("BUTTON", t, BS_PUSHBUTTON, cx - 330 + (i % 2) * 340, top + 200 + (i // 2) * 90, 320, 72, 101 + i, f_btn)
    ctl["resume"] = child("BUTTON", "Resume Work", BS_PUSHBUTTON, cx - 330, top + 400, 660, 72, 105, f_btn)
    user32.EnableWindow(ctl["resume"], False)
    tick_text()
    user32.SetTimer(hwnd, 1, 300, None)

    def kb(nCode, wParam, lParam):
        if nCode >= 0:
            k = ctypes.cast(lParam, ctypes.POINTER(KBD)).contents
            alt = bool(k.flags & 0x20)
            ctrl = bool(user32.GetAsyncKeyState(VK_CONTROL) & 0x8000)
            vk = k.vkCode
            if (vk in (VK_LWIN, VK_RWIN, VK_APPS)
                    or (alt and vk in (VK_TAB, VK_ESCAPE, VK_F4))
                    or (ctrl and vk == VK_ESCAPE)):
                return 1
        return user32.CallNextHookEx(None, nCode, wParam, lParam)
    kb_cb = HOOKPROC(kb)
    hook = user32.SetWindowsHookExW(13, kb_cb, hinst, 0)

    user32.SetForegroundWindow(hwnd)
    msg = wintypes.MSG()
    while user32.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
        user32.TranslateMessage(ctypes.byref(msg))
        user32.DispatchMessageW(ctypes.byref(msg))
    if hook:
        user32.UnhookWindowsHookEx(hook)


def _thread_main():
    try:
        _policies_apply()
        _run_window()
    finally:
        policies_restore()
        with _lock:
            state["active"] = False
        _hwnd_box["hwnd"] = None


def start(start_epoch=None):
    """Show the lock. Returns True if the full-screen lock is now up."""
    if not IS_WIN:
        return False
    with _lock:
        if state["active"]:
            return True
        state.update(active=True, resumed=False, chosen="", start=float(start_epoch or time.time()))
    threading.Thread(target=_thread_main, daemon=True, name="wfh-break-lock").start()
    return True


def stop():
    """Release the lock (used when Resume was pressed in the browser instead)."""
    if not IS_WIN:
        return
    if _hwnd_box.get("hwnd"):
        state["start"] = 0.0   # the window's 300 ms timer sees this and closes the lock itself


def idle_seconds():
    """Seconds since the last mouse/keyboard input anywhere in Windows (None when unavailable)."""
    if not IS_WIN:
        return None
    try:
        import ctypes

        class LII(ctypes.Structure):
            _fields_ = [("cbSize", ctypes.c_uint), ("dwTime", ctypes.c_uint)]
        i = LII()
        i.cbSize = ctypes.sizeof(LII)
        if not ctypes.windll.user32.GetLastInputInfo(ctypes.byref(i)):
            return None
        now = ctypes.windll.kernel32.GetTickCount() & 0xFFFFFFFF
        return ((now - i.dwTime) & 0xFFFFFFFF) // 1000
    except Exception:
        return None


def status():
    with _lock:
        return {"active": state["active"], "resumed": state["resumed"], "chosen": state["chosen"]}


def clear_result():
    with _lock:
        state["resumed"], state["chosen"] = False, ""


policies_restore()                 # undo a lock left behind by a crash
atexit.register(policies_restore)
