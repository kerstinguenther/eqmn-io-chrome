/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/

/*global chrome, console, require:true, exports:true, define:true, $, def:true, SETTINGS */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

function dependOn() {
    'use strict';
    return [
        require("util"),
        require("common"),
        require("analytics"),
        require("proxy")
    ];
}

var def;
require = function (m) {'use strict'; return m; };
if (window.define) {
    def = window.define;
} else {
    def = function (deps, fn) {
        'use strict';
        return fn.apply(null, [{
            ajax: $.ajax.bind($)
        }]);
    };
}
var exports = acom_analytics = {};

def(dependOn(), function (util, common, analytics, proxy) {
    'use strict';
    var singleton = null, prop, modules = {}, activeTab;

    function Communicate() {
        var opsHandlers = {},
            Abort,
            tabs = {};

        this.proxy = proxy.proxy.bind(this);
        this.LOG = common.LOG;

        this.registerHandlers = function (handlers) {
            util.extend(opsHandlers, handlers);
        };

        this.registerModule = function (name, module) {
            modules[name] = module;
        };

        this.getModule = function (name) {
            return modules[name];
        };

        this.reset = function (request, sender) {
            common.reset(request.environment);
            analytics.setAnalyticsUsage(request.analytics_on, sender.tab.id);
            if (request.environment) {
                analytics.environment = request.environment;
                analytics.event(analytics.e.OPTIONS_SET_ENV);
            }
        };

        this.registerHandlers(
            {reset: this.proxy(this.reset)}
        );

        // Versions:
        // -1 : unknown
        // 1 : Older than 15
        // 15: the latest
        this.version = -1;
        this.legacyShim = function () {
            return this.version <= 1;
        };
        this.setVersion = function (ver) {
            this.version = ver;
        };

        this.handler = function (request, sender, reply) {
            var options, op, params;

            if (!request) {
                // request can be undefined if the send message request failed.
                // e.g. happens for tabs that were open before the
                // extension was installed.
                return;
            }
            this.dump(request, "Communicate Handler receive: ");
            if (!sender || !sender.tab) {
                // We can end up without a proper sender parameter in
                // the case where chrome starts up with open pages.
                // We get a sender that looks like:
                // sender: {"id":"eggdicfdigbldbflldgmnfaoopngjgee" ... }
                return;
            }
            request.tabId = sender.tab.id;
            if (!activeTab) {
                activeTab = sender.tab.id;
            }

            if (!request.main_op) {
                return;
            }

            if (request.main_op === "relay_to_content") {
                // this path is for the iframe to communicate to
                // the content script.
                // Since we don't want to use window.postMessage,
                // there's no direct communication.
                // So we relay the message through here.
                delete request.main_op;
                this.sendMessage(request);
                return;
            }
            // propagate any analytics received from the browser-side
            analytics.logBrowserAnalytics(request);
            op = request.main_op;
            delete request.main_op;
            if (op === "dismiss") {
                this.closeDialog();
            }
            if (!opsHandlers[op]) {
                util.consoleLog("failed to find handler for: " + op);
                return;
            }

            analytics.setOp(
                {
                    "preview"       : "Copy",
                    "image_preview" : "Image",
                    "send"          : "Send",
                    "fillsign"      : "FillSign",
                    "export"        : "Export",
                    "acom"          : "GotoAcom",
                    "to_pdf"        : "ConvertToPdf"

                }[request.handleResult]
            );

            return opsHandlers[op](request, sender, reply);
        };

        this.closeDialog = function () {
            if (util.isFF()) {
                this.globals.panel.hide();
            }
        };

        // echo back the last request to the popup so that
        // users can revisit the last state.
        this.echo = function (tab) {
            var request, command, session, self, is_pdf;
            analytics.event(analytics.e.TREFOIL_CLICKED);
            // last chance check to make sure we haven't somehow ended up
            // with the extension enabled on a url we can't process
            if (this.avoidUrl(tab.url)) {
                this.disable(tab.id);
                return;
            }
            if (util.isFF()) {
                request = this.lastRequest || {panel_op: "html_menu"};
            } else {
                // chrome messages are tab-specific
                if (tabs[tab.id]) {
                    request = tabs[tab.id];
                    // don't repeat a "cancel" status
                    if (request.current_status === "cancelled" ||
                            request.current_status === "pdf_opened") {

                        is_pdf = request.is_pdf;
                        this.clearStatus(request);
                        // clearStatus is pretty aggressive --
                        // don't clear the is_pdf state
                        request.is_pdf = is_pdf;
                    }
                    if (request.current_status) {
                        request.panel_op = "status";
                    }
                    self = this;
                    //if this is mac redirect user to options page
                    if (SETTINGS.OS === "mac") {
                        session = self.getModule('session');
                        session.newSession("data/js/options.html?os=mac",
                                           true, {});
                        return;
                    }
                    command = request.is_pdf ? "pdf_menu" : "html_menu";
                    request.panel_op = request.panel_op || command;
                    util.consoleLog(
                        "repeat cached request: " + request.panel_op
                    );
                }
            }
            if (request) {
                request.trefoilClick = true;
                this.sendMessage(request);
            }
        };

        this.setGlobals = function (globals) {
            this.globals = globals;
        };

        this.dump = function (msg, title) {
            var p, o = [title];
            for (p in msg) {
                if (msg.hasOwnProperty(p)) {
                    o.push("  " + p + ": " + msg[p]);
                }
            }
            util.consoleLog(o.join("\n"));
        };


        // Message passing
        // messages will be targeted at one of our three environments:
        // request.main_op is an operation for the main (background)
        // part of the extension
        // request.content_op is an operation for the injected content script
        // request.panel_op is an operation for the iframe (chrome)
        // panel (firefox)
        // request.progress_op is an operation for the progress page
        // request.html_op is an operation for the content script that
        //                 generates self-contained html
        this.sendMessage = function (request) {
            var tabId = request.tabId,
                analytics_string;

            this.dump(request, "Sending message:");

            request.version = this.version;

            tabs[tabId] = util.extend(tabs[tabId], request);

            if (request.panel_op === "pdf_menu") {
                if (request.trefoilClick) {
                    analytics_string = analytics.e.TREFOIL_PDF_FROM_CLICK;
                } else {
                    analytics_string = analytics.e.TREFOIL_PDF_MENU_SHOWN;
                }
            }
            if (request.panel_op === "flickr") {
                analytics_string = analytics.e.FLICKR_OFFER_SHOWN;
            }
            if (request.panel_op === "html_menu") {
                analytics_string = analytics.e.TREFOIL_HTML_FROM_CLICK;
            }
            if (analytics_string) {
                analytics.checkAndLogAnalytics(analytics_string);
            }

            util.sendMessage(request, this.globals);
            delete request.trefoilClick;
        };

        this.deferMessage = function (request) {
            // strangely ...
            // in firefox the xhr callback can lose global scope ...
            // and won't have a setTimeout function.
            if (typeof (setTimeout) === "undefined") {
                this.sendMessage(request);
            } else {
                setTimeout(this.proxy(this.sendMessage, request), 0);
            }
        };

        this.filenameFromUrl = function (url) {
            var filename = decodeURIComponent(url.replace(/\S*\//, ""));
            return filename.replace(/[\?#]\S*/, "");

        };

        this.pdf_menu = function (request, sender) {
            var self = this, session, r;
            //if this is mac redirect user to options page
            if (SETTINGS.OS === "mac") {
                return;
            }
            r = tabs[sender.tab.id] =
                util.extend(tabs[sender.tab.id],
                            {tabId: sender.tab.id});

            r.filename = self.filenameFromUrl(request.url);
            r.panel_op = "pdf_menu";
            r.url = request.url;
            r.is_pdf = true;
            // make sure the user hasn't turned off the auto display
            if (util.getCookie("always-show-pdf-menu") !== "false") {
                self.deferMessage(r);
            }
        };

        this.loaded = function (tabId) {
            tabs[tabId] = util.extend(
                tabs[tabId],
                {tabId: tabId, loaded: true}
            );
            this.enable(tabId);
        };

        this.clearStatus = function (r, clearWaiting) {
            if (r.current_status === "in_progress" ||
                    r.current_status === "downloading") {
                return;
            }
            if (!clearWaiting && r.current_status === "waiting") {
                return;
            }
            this.getModule("acro-web2pdf").cancelConversion(r.tabId);
            delete r.current_status;
            delete r.file_path;
            delete r.domtitle;
            delete r.timing;
            delete r.panel_op;
            delete r.is_pdf;
        };

        this.loading = function (tab) {
            var tabId = tab.id;
            tabs[tabId] = util.extend(
                tabs[tabId],
                {tabId: tabId, loaded: false}
            );
            this.clearStatus(tabs[tabId], true);
            this.enable(tabId);
        };

        this.active = function (tab) {
            activeTab = tab.tabId;
            tabs[activeTab] = util.extend(
                tabs[activeTab],
                {tabId: tab.tabId}
            );
        };

        this.enable = function (tabId) {
            var image = tabs[tabId].loaded ?
                    "data/images/acrobat_dc_appicon_24.png" :
                    "data/images/acrobat_dc_appicon_24_translucent.png";
            chrome.browserAction.setIcon({path: image, tabId: tabId});
            if (tabs[tabId].loaded) {
                chrome.browserAction.enable(tabId);
            } else {
                chrome.browserAction.disable(tabId);
            }
        };

        this.disable = function (tabId) {
            if (tabs[tabId]) {
                // make sure we don't try to process this tab...
                tabs[tabId].loaded = false;
                this.enable(tabId);
            }
        };

        this.close = function (tabId) {
            this.getModule("acro-web2pdf").cancelConversion(tabId);
            delete tabs[tabId];
            if (activeTab === tabId) {
                activeTab = null;
            }
        };

        this.tabReplace = function (newTab, oldTab) {
            this.close(oldTab);
            this.loaded(newTab);
        };

        this.activeTab = function () {
            return activeTab;
        };

        this.noop = function () {};

        this.avoidUrl = function (url) {
            url = url || "";
            // somehow the chrome store site blocks
            // our content script.  So don't even try...
            if (url.startsWith("https://chrome.google.com")) {
                return true;
            }
            return !url.startsWith("http");
        };


    }

    if (!singleton) {
        singleton = new Communicate();

        if (util.isChrome()) {
            chrome
                .runtime
                .onMessage
                .addListener(singleton.proxy(singleton.handler));

            chrome
                .tabs
                .onActivated
                .addListener(singleton.proxy(singleton.active));

            chrome
                .tabs
                .onRemoved
                .addListener(singleton.proxy(singleton.close));

            chrome
                .tabs
                .onCreated
                .addListener(singleton.proxy(singleton.loading));

            chrome
                .tabs
                .onReplaced
                .addListener(singleton.proxy(singleton.tabReplace));
        }
    }
    for (prop in singleton) {
        if (singleton.hasOwnProperty(prop)) {
            if (typeof (singleton[prop]) === "function") {
                exports[prop] = singleton[prop].bind(singleton);
            } else {
                exports[prop] = singleton[prop];
            }
        }
    }
    singleton.registerHandlers(
        {
            "send-analytics"        : singleton.proxy(singleton.noop)
        }
    );
    return singleton;
});
