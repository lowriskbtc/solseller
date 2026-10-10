<#
    Pin the TRADE BOX window on top and/or make it see-through.

    Why this exists: Chrome app-mode (--app) gives a proper window - no address
    bar, drags and resizes normally - but it is an ordinary opaque window, so
    clicking the browser sends it behind and it hides the chart underneath.
    Both fixes are OS-level window attributes, not anything the page can do:

      WS_EX_TOPMOST     keeps it above the browser
      WS_EX_LAYERED     + SetLayeredWindowAttributes = real transparency

    CSS cannot do the second one. An rgba() background composites against the
    browser's own backdrop, not against whatever window is behind it, so the
    page would look faded but you still would not see the chart through it.

    Window lookup is EnumWindows, not Process.MainWindowHandle: one Chrome
    process hosts every Chrome window and MainWindowHandle returns whichever it
    considers primary, which with a couple of tabs open is not the trade box.
    Matching is exact-title first, then prefix - so it finds the bar-free Chrome
    app window ("TRADE BOX") in preference to a browser popup of the same page
    ("TRADE BOX - DuckDuckGo"), but still works on the popup if that is all
    that is open.

    Usage:
      pin_topmost.ps1                 pin on top, 85% opaque
      pin_topmost.ps1 -Alpha 255      pin on top, fully opaque
      pin_topmost.ps1 -Alpha 170      pin on top, more see-through (67%)
      pin_topmost.ps1 -Off            unpin and restore full opacity

    Alpha is 0-255. Below about 140 the text starts getting hard to read
    against a busy chart - 200-230 is the usable range.
#>
param(
    [string]$Title = 'TRADE BOX',
    [ValidateRange(0,255)][int]$Alpha = 217,      # ~85%
    [switch]$Off,
    [int]$TimeoutSec = 15
)

Add-Type -Namespace T8 -Name Win -MemberDefinition @'
[DllImport("user32.dll")]
public static extern bool EnumWindows(EnumWindowsProc cb, IntPtr lParam);
public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);
[DllImport("user32.dll", CharSet=CharSet.Unicode)]
public static extern int GetWindowTextW(IntPtr hWnd, System.Text.StringBuilder s, int n);
[DllImport("user32.dll")]
public static extern bool IsWindowVisible(IntPtr hWnd);
[DllImport("user32.dll")]
public static extern bool SetWindowPos(IntPtr hWnd, IntPtr after, int x, int y, int cx, int cy, uint flags);
[DllImport("user32.dll")]
public static extern IntPtr GetWindowLongPtrW(IntPtr hWnd, int index);
[DllImport("user32.dll")]
public static extern IntPtr SetWindowLongPtrW(IntPtr hWnd, int index, IntPtr val);
[DllImport("user32.dll")]
public static extern bool SetLayeredWindowAttributes(IntPtr hWnd, uint key, byte alpha, uint flags);
[DllImport("user32.dll")]
public static extern bool GetLayeredWindowAttributes(IntPtr hWnd, out uint key, out byte alpha, out uint flags);

// exact title wins; otherwise first window whose title starts with it
public static IntPtr FindByTitle(string want) {
    IntPtr exact = IntPtr.Zero, prefix = IntPtr.Zero;
    EnumWindows(delegate(IntPtr h, IntPtr l) {
        if (!IsWindowVisible(h)) return true;
        System.Text.StringBuilder sb = new System.Text.StringBuilder(512);
        GetWindowTextW(h, sb, sb.Capacity);
        string t = sb.ToString();
        if (t == want) { exact = h; return false; }
        if (prefix == IntPtr.Zero && t.StartsWith(want)) prefix = h;
        return true;
    }, IntPtr.Zero);
    return (exact != IntPtr.Zero) ? exact : prefix;
}

public static string TitleOf(IntPtr h) {
    System.Text.StringBuilder sb = new System.Text.StringBuilder(512);
    GetWindowTextW(h, sb, sb.Capacity);
    return sb.ToString();
}
'@

$GWL_EXSTYLE   = -20
$WS_EX_TOPMOST = 0x8
$WS_EX_LAYERED = 0x80000
$LWA_ALPHA     = 0x2
$HWND_TOPMOST    = [IntPtr](-1)
$HWND_NOTOPMOST  = [IntPtr](-2)
# SWP_NOSIZE|SWP_NOMOVE|SWP_NOACTIVATE - never move, resize, or steal focus
$SWP = 0x13

$deadline = (Get-Date).AddSeconds($TimeoutSec)
$hwnd = [IntPtr]::Zero
while ((Get-Date) -lt $deadline) {
    $hwnd = [T8.Win]::FindByTitle($Title)
    if ($hwnd -ne [IntPtr]::Zero) { break }
    Start-Sleep -Milliseconds 400          # window is still being created
}
if ($hwnd -eq [IntPtr]::Zero) {
    Write-Host "no visible window titled '$Title' - is the trade box open?"
    exit 1
}
$found = [T8.Win]::TitleOf($hwnd)

if ($Off) {
    [void][T8.Win]::SetWindowPos($hwnd, $HWND_NOTOPMOST, 0, 0, 0, 0, $SWP)
    # drop WS_EX_LAYERED entirely rather than setting alpha 255: leaving a
    # window layered keeps it on a compositing path it does not otherwise need.
    $ex = [T8.Win]::GetWindowLongPtrW($hwnd, $GWL_EXSTYLE).ToInt64()
    [void][T8.Win]::SetWindowLongPtrW($hwnd, $GWL_EXSTYLE, [IntPtr]($ex -band (-bnot $WS_EX_LAYERED)))
    Write-Host "'$found' unpinned and fully opaque - a normal window again"
    exit 0
}

[void][T8.Win]::SetWindowPos($hwnd, $HWND_TOPMOST, 0, 0, 0, 0, $SWP)

$ex = [T8.Win]::GetWindowLongPtrW($hwnd, $GWL_EXSTYLE).ToInt64()
if (($ex -band $WS_EX_LAYERED) -eq 0) {
    [void][T8.Win]::SetWindowLongPtrW($hwnd, $GWL_EXSTYLE, [IntPtr]($ex -bor $WS_EX_LAYERED))
}
$ok = [T8.Win]::SetLayeredWindowAttributes($hwnd, 0, [byte]$Alpha, $LWA_ALPHA)
if (-not $ok) {
    Write-Host "'$found' pinned on top, but transparency was refused by the window"
    exit 1
}

$pct = [math]::Round($Alpha / 255 * 100)
Write-Host "'$found' pinned on top at $pct% opacity (alpha $Alpha)"
exit 0
