import AppKit
import CoreGraphics
import Foundation

if CommandLine.arguments.count < 3 {
    fputs("Usage: capture_frontmost_window.swift <owner_name> <output_png>\n", stderr)
    exit(2)
}

let ownerName = CommandLine.arguments[1]
let outputURL = URL(fileURLWithPath: CommandLine.arguments[2])

guard
    let windowList = CGWindowListCopyWindowInfo([.optionOnScreenOnly, .excludeDesktopElements], kCGNullWindowID)
        as? [[String: Any]]
else {
    fputs("Could not read on-screen window list.\n", stderr)
    exit(1)
}

guard
    let window = windowList.first(where: { row in
        String(describing: row[kCGWindowOwnerName as String] ?? "") == ownerName
            && (row[kCGWindowLayer as String] as? Int ?? 1) == 0
    }),
    let windowNumber = window[kCGWindowNumber as String] as? UInt32
else {
    fputs("No visible window found for \(ownerName).\n", stderr)
    exit(1)
}

guard
    let image = CGWindowListCreateImage(
        .null,
        .optionIncludingWindow,
        CGWindowID(windowNumber),
        [.bestResolution, .boundsIgnoreFraming]
    )
else {
    fputs("Failed to capture window image for \(ownerName).\n", stderr)
    exit(1)
}

let bitmap = NSBitmapImageRep(cgImage: image)
guard let pngData = bitmap.representation(using: .png, properties: [:]) else {
    fputs("Failed to convert captured image to PNG.\n", stderr)
    exit(1)
}

do {
    try FileManager.default.createDirectory(
        at: outputURL.deletingLastPathComponent(),
        withIntermediateDirectories: true
    )
    try pngData.write(to: outputURL)
    print(outputURL.path)
} catch {
    fputs("Failed to write PNG: \(error.localizedDescription)\n", stderr)
    exit(1)
}
