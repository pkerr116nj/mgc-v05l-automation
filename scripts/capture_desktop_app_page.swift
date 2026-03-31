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
            let titles = String(describing: state["titles"] ?? "")
            fputs("poll attempt \(self.attempts): rows=\(rowCount) markers=\(hasAllMarkers) titles=\(titles)\n", stderr)
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
let markerTexts: [String] = {
    if let raw = ProcessInfo.processInfo.environment["CAPTURE_MARKERS"], !raw.isEmpty {
        return raw.split(separator: "|").map { String($0) }
    }
    return [
        "ATPE Long Medium+High Canary",
        "ATPE Short High-Only Canary",
        "MGC / usLatePauseResumeLongTurn",
    ]
}()
let proofTitle = ProcessInfo.processInfo.environment["CAPTURE_PROOF_TITLE"] ?? "Rendered Desktop App Proof: Strategies"
let proofSubtitle = ProcessInfo.processInfo.environment["CAPTURE_PROOF_SUBTITLE"]
    ?? "ATPE is rendered inline in the same desktop-app strategy tables as the regular probationary paper strategies."
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
  const sections = Array.from(document.querySelectorAll('.section-card'));
  const byTitle = (title) => sections.find((section) => section.querySelector('.section-title')?.textContent?.trim() === title);
  const registry = byTitle('Standalone Strategy Registry');
  const liveEligibility = byTitle('Live Eligibility');
  const strategyPerformance = byTitle('Strategy Performance');
  const target = registry?.querySelector('table');
  if (registry && target && !document.body.dataset.capturePrepared) {
    const wrapper = document.createElement('div');
    wrapper.className = 'capture-proof-root';
    wrapper.style.padding = '28px';
    wrapper.style.maxWidth = '1500px';
    wrapper.style.margin = '0 auto';

    const heading = document.createElement('section');
    heading.className = 'section-card';
    heading.style.marginBottom = '18px';
    heading.innerHTML = `
      <div class="section-header">
        <div>
          <div class="section-title">\(proofTitle.replacingOccurrences(of: "'", with: "\\'"))</div>
          <div class="section-subtitle">\(proofSubtitle.replacingOccurrences(of: "'", with: "\\'"))</div>
        </div>
      </div>
    `;

    const expandSection = (section) => {
      const clone = section.cloneNode(true);
      clone.querySelectorAll('tbody').forEach((tbody) => {
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const focusedRows = rows.filter((row, index) => [\(markerTexts.map { "'\($0.replacingOccurrences(of: "'", with: "\\'"))'" }.joined(separator: ","))].some((marker) => row.innerText.includes(marker)) || index < 2);
        if (focusedRows.length && focusedRows.length < rows.length) {
          tbody.replaceChildren(...focusedRows);
        }
      });
      clone.querySelectorAll('table').forEach((node) => {
        node.style.fontSize = '11px';
      });
      return clone;
    };

    const registryClone = expandSection(registry);
    registryClone.querySelectorAll('table').forEach((node) => {
      node.style.fontSize = '11px';
    });

    wrapper.appendChild(heading);
    wrapper.appendChild(registryClone);
    if (liveEligibility) {
      wrapper.appendChild(expandSection(liveEligibility));
    }
    if (strategyPerformance) {
      wrapper.appendChild(expandSection(strategyPerformance));
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
    titles: sections.map((section) => section.querySelector('.section-title')?.textContent?.trim()).filter(Boolean).join(' | '),
  };
})()
"""

let delegate = SnapshotDelegate(
    webView: webView,
    outputURL: outputURL,
    markerTexts: markerTexts,
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
