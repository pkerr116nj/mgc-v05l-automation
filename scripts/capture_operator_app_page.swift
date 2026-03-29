import AppKit
import Foundation
import WebKit

final class SnapshotDelegate: NSObject, WKNavigationDelegate {
    private let webView: WKWebView
    private let outputURL: URL
    private let markerTexts: [String]
    private let readyScript: String
    private var attempts = 0
    private let maxAttempts = 20

    init(webView: WKWebView, outputURL: URL, markerTexts: [String], readyScript: String) {
        self.webView = webView
        self.outputURL = outputURL
        self.markerTexts = markerTexts
        self.readyScript = readyScript
        super.init()
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        fputs("didFinish navigation\n", stderr)
        pollUntilReady()
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        finishWithError("Navigation failed: \(error.localizedDescription)")
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        finishWithError("Provisional navigation failed: \(error.localizedDescription)")
    }

    private func pollUntilReady() {
        attempts += 1
        webView.evaluateJavaScript(readyScript) { [weak self] result, error in
            guard let self else { return }
            if let error {
                self.finishWithError("Page probe failed: \(error.localizedDescription)")
                return
            }
            guard let state = result as? [String: Any] else {
                self.retryOrFail(reason: "Page probe returned no state.")
                return
            }
            let tableText = String(describing: state["tableText"] ?? "")
            let hasAllMarkers = self.markerTexts.allSatisfy { tableText.contains($0) }
            let rowCount = Int(String(describing: state["rowCount"] ?? "0")) ?? 0
            fputs("poll attempt \(self.attempts): rows=\(rowCount) markers=\(hasAllMarkers)\n", stderr)
            if (hasAllMarkers && rowCount > 0) || rowCount >= 7 {
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                    self.captureSnapshot()
                }
            } else {
                self.retryOrFail(reason: "Waiting for rendered rows (attempt \(self.attempts)/\(self.maxAttempts)).")
            }
        }
    }

    private func retryOrFail(reason: String) {
        if attempts >= maxAttempts {
            finishWithError(reason)
            return
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
            self.pollUntilReady()
        }
    }

    private func captureSnapshot() {
        let config = WKSnapshotConfiguration()
        webView.takeSnapshot(with: config) { [weak self] image, error in
            guard let self else { return }
            if let error {
                self.finishWithError("Snapshot failed: \(error.localizedDescription)")
                return
            }
            guard
                let image,
                let tiff = image.tiffRepresentation,
                let bitmap = NSBitmapImageRep(data: tiff),
                let png = bitmap.representation(using: .png, properties: [:])
            else {
                self.finishWithError("Snapshot returned no image data.")
                return
            }
            do {
                try FileManager.default.createDirectory(
                    at: self.outputURL.deletingLastPathComponent(),
                    withIntermediateDirectories: true
                )
                try png.write(to: self.outputURL)
                print(self.outputURL.path)
                NSApp.terminate(nil)
            } catch {
                self.finishWithError("Failed to write snapshot: \(error.localizedDescription)")
            }
        }
    }

    private func finishWithError(_ message: String) {
        fputs(message + "\n", stderr)
        NSApp.terminate(nil)
        exit(1)
    }
}

if CommandLine.arguments.count < 3 {
    fputs("Usage: capture_operator_app_page.swift <output_png> <url>\n", stderr)
    exit(2)
}

let outputURL = URL(fileURLWithPath: CommandLine.arguments[1])
let pageURL = URL(string: CommandLine.arguments[2])!
fputs("loading \(pageURL.absoluteString)\n", stderr)
let app = NSApplication.shared
app.setActivationPolicy(.prohibited)

let window = NSWindow(
    contentRect: CGRect(x: 0, y: 0, width: 1440, height: 1180),
    styleMask: [.titled],
    backing: .buffered,
    defer: false
)
let webView = WKWebView(frame: window.contentView!.bounds)
webView.autoresizingMask = [.width, .height]
window.contentView?.addSubview(webView)

let readyScript = """
(() => {
  const target = document.getElementById('approved-models-table');
  const paperCard = target ? target.closest('.table-card') : null;
  const activityCard = document.getElementById('paper-lane-activity-table')?.closest('.table-card');
  if (paperCard && !document.body.dataset.capturePrepared) {
    const wrapper = document.createElement('div');
    wrapper.className = 'capture-proof-root';
    wrapper.style.padding = '28px';
    wrapper.style.maxWidth = '1320px';
    wrapper.style.margin = '0 auto';

    const heading = document.createElement('div');
    heading.className = 'panel';
    heading.style.marginBottom = '18px';
    heading.innerHTML = `
      <div class="panel-header">
        <h2>Rendered App Proof: Paper Strategies</h2>
        <div class="panel-actions">
          <span class="badge badge-muted">TEMPORARY PAPER STRATEGIES INLINE</span>
        </div>
      </div>
      <div class="subnote model-scope-note"><strong>Proof Goal:</strong> ATPE appears in the same in-app paper strategy surfaces as the regular paper strategies.</div>
    `;

    const paperClone = paperCard.cloneNode(true);
    paperClone.querySelectorAll('.table-wrap').forEach((node) => {
      node.style.maxHeight = 'none';
      node.style.overflow = 'visible';
    });
    paperClone.querySelectorAll('table').forEach((node) => {
      node.style.fontSize = '11px';
    });

    wrapper.appendChild(heading);
    wrapper.appendChild(paperClone);
    if (activityCard) {
      const activityClone = activityCard.cloneNode(true);
      activityClone.querySelectorAll('.table-wrap').forEach((node) => {
        node.style.maxHeight = 'none';
        node.style.overflow = 'visible';
      });
      activityClone.querySelectorAll('table').forEach((node) => {
        node.style.fontSize = '11px';
      });
      wrapper.appendChild(activityClone);
    }

    document.body.innerHTML = '';
    document.body.appendChild(wrapper);
    document.body.dataset.capturePrepared = 'true';
    document.body.style.minHeight = 'auto';
    document.body.style.padding = '0';
    window.scrollTo(0, 0);
  }
  return {
    rowCount: target ? target.querySelectorAll('tbody tr').length : 0,
    tableText: target ? target.innerText : '',
  };
})()
"""

let delegate = SnapshotDelegate(
    webView: webView,
    outputURL: outputURL,
    markerTexts: [
        "ATPE Long Medium+High Canary",
        "ATPE Short High-Only Canary",
    ],
    readyScript: readyScript
)
webView.navigationDelegate = delegate
window.orderFrontRegardless()
if pageURL.isFileURL {
    let readAccessURL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
    webView.loadFileURL(pageURL, allowingReadAccessTo: readAccessURL)
} else {
    webView.load(URLRequest(url: pageURL))
}
app.run()
