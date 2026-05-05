package com.salonkarola.app;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.webkit.SslErrorHandler;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;

import com.getcapacitor.Bridge;
import com.getcapacitor.BridgeActivity;
import com.getcapacitor.BridgeWebViewClient;

import java.net.URISyntaxException;

public class MainActivity extends BridgeActivity {
    private static final String TAG = "SalonKarolaMainActivity";

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        try {
            configureWebViewSafety();
        } catch (Exception error) {
            Log.e(TAG, "WebView-Sicherheitssetup fehlgeschlagen", error);
        }
    }

    private void configureWebViewSafety() {
        if (bridge == null || bridge.getWebView() == null) return;
        WebView webView = bridge.getWebView();
        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_NEVER_ALLOW);
        webView.setWebViewClient(new SafeBridgeWebViewClient(bridge));
    }

    private class SafeBridgeWebViewClient extends BridgeWebViewClient {
        SafeBridgeWebViewClient(Bridge bridge) {
            super(bridge);
        }

        @Override
        public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
            String url = request != null && request.getUrl() != null ? request.getUrl().toString() : "";
            if (openExternalIfNeeded(url)) return true;
            return super.shouldOverrideUrlLoading(view, request);
        }

        @Override
        public boolean shouldOverrideUrlLoading(WebView view, String url) {
            if (openExternalIfNeeded(url)) return true;
            return super.shouldOverrideUrlLoading(view, url);
        }

        @Override
        public void onReceivedError(WebView view, WebResourceRequest request, WebResourceError error) {
            super.onReceivedError(view, request, error);
            if (request == null || request.isForMainFrame()) {
                loadFallback(view);
            }
        }

        @Override
        public void onReceivedSslError(WebView view, SslErrorHandler handler, android.net.http.SslError error) {
            try {
                if (handler != null) handler.cancel();
            } catch (Exception ignored) {}
            loadFallback(view);
        }

        private boolean openExternalIfNeeded(String rawUrl) {
            if (rawUrl == null || rawUrl.trim().isEmpty()) return false;
            String lower = rawUrl.toLowerCase();
            try {
                if (lower.startsWith("tel:") || lower.startsWith("mailto:") || lower.startsWith("sms:") || lower.startsWith("whatsapp:")) {
                    startActivity(new Intent(Intent.ACTION_VIEW, Uri.parse(rawUrl)));
                    return true;
                }
                if (lower.startsWith("intent:")) {
                    Intent intent = Intent.parseUri(rawUrl, Intent.URI_INTENT_SCHEME);
                    intent.addCategory(Intent.CATEGORY_BROWSABLE);
                    startActivity(intent);
                    return true;
                }
                if (!(lower.startsWith("http:") || lower.startsWith("https:") || lower.startsWith("about:") || lower.startsWith("file:"))) {
                    return true;
                }
            } catch (URISyntaxException syntaxError) {
                Log.e(TAG, "Intent-URL konnte nicht geparst werden", syntaxError);
                return true;
            } catch (Exception openError) {
                Log.e(TAG, "Externe URL konnte nicht geoeffnet werden", openError);
                return true;
            }
            return false;
        }

        private void loadFallback(WebView view) {
            if (view == null) return;
            String html = "<!doctype html><html lang='de'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Salon Karola App</title>"
                    + "<style>body{font-family:Arial,sans-serif;background:#f4efe8;color:#1f1f1f;padding:20px}button,a{display:inline-block;margin:6px 8px 0 0;padding:10px 14px;border:1px solid #222;border-radius:10px;background:#fff;color:#111;text-decoration:none;font-weight:600}</style>"
                    + "</head><body><h1>Die Salon Karola App konnte nicht geladen werden.</h1><p>Bitte versuche es erneut oder starte den sicheren Modus.</p>"
                    + "<button onclick='location.reload()'>Erneut versuchen</button>"
                    + "<a href='/safe-start?safe=1'>Sicherer Start</a>"
                    + "<button onclick='(async function(){try{if(\"serviceWorker\" in navigator){const regs=await navigator.serviceWorker.getRegistrations();await Promise.all(regs.map(r=>r.unregister().catch(()=>false)));}if(\"caches\" in window){const keys=await caches.keys();await Promise.all(keys.map(k=>caches.delete(k).catch(()=>false)));}localStorage.setItem(\"sk_sw_disabled_debug\",\"1\");alert(\"Cache geloescht\");}catch(e){alert(\"Cache konnte nicht geloescht werden\");}})()'>Cache loeschen</button>"
                    + "</body></html>";
            try {
                view.loadDataWithBaseURL("https://salon-karola-app.onrender.com", html, "text/html", "UTF-8", null);
            } catch (Exception error) {
                Log.e(TAG, "Fallback-Seite konnte nicht geladen werden", error);
            }
        }
    }
}
