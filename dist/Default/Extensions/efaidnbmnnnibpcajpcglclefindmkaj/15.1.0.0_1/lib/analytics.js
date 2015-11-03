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

/*global console, require:true, exports:true, define:true, def:true, SETTINGS, AppMeasurement, chrome */
/*jslint devel: true, browser: true, nomen: true, maxlen: 118 */

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

function dependOn() {
    'use strict';
    return [
        require("util"),
        require("proxy")
    ];
}


function error(msg) {
    'use strict';
    if (SETTINGS.DEBUG_MODE) {
        console.error(msg);
    }
}

def(dependOn(), function (util, proxy) {
    'use strict';
    var singleton = null, prop, Analytics, logAnalytics = true;

    Analytics = function () {

        if (proxy) {
            this.proxy = proxy.proxy.bind(this);
        }
        function formatRequest(context, page_name, params) {
            page_name = page_name
                        .replace(/OPERATION/, context.operation || "Unknown")
                        .replace(/ENVIRONMENT/, context.environment)
                        .replace(/STAGE/, context.stage)
                        .replace(/TIMING/, context.timing)
                        .replace(/RANGE/, context.size)
                        .replace(/SITE/, context.site)
                        .replace(/PROTOCOL/, context.protocol);

            util.each(params, function (name, value) {
                page_name = page_name.replace(name, value);
            });

            if (SETTINGS.ANALYTICS) {
                console.log("%c" + page_name, "color: #800080");
            }

            // We can use properties 1-5
            // If we want more, we need them configured
            return {
                eVar1: context.version,
                eVar2: context.installType,
                eVar3: context.environment,
                eVar4: context.tireKick,
                pageName: page_name
            };
        }


        this.e = {
            EXTENSION_INSTALLED                : "DCBrowserExt:Extension:Installed:Op",
            EXTENSION_UPDATE                   : "DCBrowserExt:Extension:Update:Op",
            EXTENSION_STARTUP                  : "DCBrowserExt:Extension:Startup:Op",
            EXTENSION_FORCE_UNINSTALL          : "DCBrowserExt:Extension:ForceUninstall:Op",
            SIGN_IN_SHOWN                      : "DCBrowserExt:SignIn:OPERATION:Shown",
            SIGN_IN_COMPLETE                   : "DCBrowserExt:SignIn:OPERATION:Complete:Op",
            SIGN_IN_ABANDONED                  : "DCBrowserExt:SignIn:Abandoned:Op",
            SIGN_OUT_CLICKED                   : "DCBrowserExt:SignOut:Clicked",
            FLICKR_OFFER_SHOWN                 : "DCBrowserExt:Flickr:Offer:Shown",
            FLICKR_OFFER_CLICKED               : "DCBrowserExt:Flickr:Offer:Clicked",
            FLICKR_CONTEXT_CLICK               : "DCBrowserExt:Flickr:Context:Clicked",
            CONTEXT_UPLOAD_PDF_PAGE            : "DCBrowserExt:Context:Upload:PdfPage:Clicked",
            CONTEXT_UPLOAD_LINK                : "DCBrowserExt:Context:Upload:Link:Clicked",
            CONTEXT_UPLOAD_IMAGE               : "DCBrowserExt:Context:Upload:Image:Clicked",
            CONTEXT_MENU_CONVERT_PAGE          : "DCBrowserExt:ContextMenu:ConvertPage:Clicked",
            CONTEXT_MENU_CONVERT_LINK          : "DCBrowserExt:ContextMenu:ConvertLink:Clicked",
            CONTEXT_MENU_APPEND_PAGE           : "DCBrowserExt:ContextMenu:AppendPage:Clicked",
            CONTEXT_MENU_APPEND_LINK           : "DCBrowserExt:ContextMenu:AppendLink:Clicked",
            REDIRECT                           : "DCBrowserExt:Redirect:OPERATION:Op",
            PDF_MENU_UPLOAD_COMPLETE_SHOWN     : "DCBrowserExt:PDF:Menu:OPERATION:Upload:Complete:Shown",
            PDF_MENU_UPLOAD_CLICKED            : "DCBrowserExt:PDF:Menu:Copy:Clicked",
            PDF_MENU_EXPORT_CLICKED            : "DCBrowserExt:PDF:Menu:Export:Clicked",
            PDF_MENU_SEND_CLICKED              : "DCBrowserExt:PDF:Menu:Send:Clicked",
            PDF_MENU_FILLSIGN_CLICKED          : "DCBrowserExt:PDF:Menu:FillSign:Clicked",
            PDF_SOURCE_SIZE                    : "DCBrowserExt:PDF:Source:Size:RANGE:Op",
            TREFOIL_CLICKED                    : "DCBrowserExt:Trefoil:Clicked",
            TREFOIL_PDF_MENU_SHOWN             : "DCBrowserExt:TrefoilMenu:PDF:Popup:Shown",
            TREFOIL_PDF_FROM_CLICK             : "DCBrowserExt:TrefoilMenu:PDF:FromClick:TIREKICK:Shown",
            TREFOIL_PDF_VISIT_AIC              : "DCBrowserExt:TrefoilMenu:PDF:VisitAIC:TIREKICK:Clicked",
            TREFOIL_PDF_ACROBAT                : "DCBrowserExt:TrefoilMenu:PDF:OpenInAcrobat:TIREKICK:Clicked",
            TREFOIL_PDF_DOWNLOAD_OPENED        : "DCBrowserExt:TrefoilMenu:PDF:OpenInAcrobat:Complete:Op",
            TREFOIL_PDF_DOWNLOAD_OPEN_FAILED   : "DCBrowserExt:TrefoilMenu:PDF:OpenInAcrobat:Failed:Op",
            TREFOIL_HTML_FROM_CLICK            : "DCBrowserExt:TrefoilMenu:HTML:FromClick:Shown",
            TREFOIL_HTML_OPTIONS_FROM_CLICK    : "DCBrowserExt:TrefoilMenu:HTML:WithOptions:FromClick:Shown",
            TREFOIL_HTML_VISIT_AIC             : "DCBrowserExt:TrefoilMenu:HTML:VisitAIC:TIREKICK:Clicked",
            TREFOIL_HTML_PREFERENCES_CLICK     : "DCBrowserExt:TrefoilMenu:HTML:Preferences:Clicked",
            TREFOIL_HTML_CONVERT_NEW           : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:New:TIREKICK:Clicked",
            TREFOIL_HTML_CONVERT_APPEND        : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:Append:TIREKICK:Clicked",
            TREFOIL_HTML_CONVERT_NO_OPEN       : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:NoOpen:Clicked",
            TREFOIL_HTML_CONVERT_OPEN_DEFAULT  :
                "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:WithOpen:Default:Clicked",
            TREFOIL_HTML_CONVERT_OPEN_CHANGED  :
                "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:WithOpen:Changed:Clicked",
            TREFOIL_HTML_CONVERT_WAITING       : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:Waiting:Shown",
            TREFOIL_HTML_CONVERT_DOWNLOADING   : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:Downloading:Shown",
            TREFOIL_HTML_CONVERT_IN_PROGRESS   : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:InProgress:Shown",
            TREFOIL_HTML_CONVERT_CANCELLED     : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:Cancelled:Shown",
            TREFOIL_HTML_CONVERT_COMPLETE      : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:Complete:Shown",
            TREFOIL_HTML_CONVERT_FAILED        : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:Failed:Shown",
            TREFOIL_HTML_CONVERT_NO_ACROBAT    : "DCBrowserExt:TrefoilMenu:HTML:ConvertHTML:NoAcrobat:Shown",
            TREFOIL_HTML_OPENPDF_PREF_ON       : "DCBrowserExt:TrefoilMenu:HTML:OpenPDFPref:On:Clicked",
            TREFOIL_HTML_OPENPDF_PREF_OFF      : "DCBrowserExt:TrefoilMenu:HTML:OpenPDFPref:Off:Clicked",

            UPLOAD_PROGRESS_SHOWN              : "DCBrowserExt:Upload:OPERATION:Progress:Shown",
            CREATE_FORM_PROGRESS_SHOWN         : "DCBrowserExt:Upload:FillSign:CreateForm:Progress:Shown",
            UPLOAD_COMPLETE                    : "DCBrowserExt:Upload:OPERATION:Complete:Op",
            CREATE_FORM_COMPLETE               : "DCBrowserExt:Upload:FillSign:CreateForm:Complete:Op",
            UPLOAD_RENAME_CLICKED              : "DCBrowserExt:Upload:RenameOrMove:Clicked",
            ERROR_SHOWN                        : "DCBrowserExt:Error:Shown",
            ERROR_WRONG_MIME_TYPE              : "DCBrowserExt:Error:WrongMimeType:Shown",
            OPTIONS_SET_ENV                    : "DCBrowserExt:Options:SetEnv:ENVIRONMENT:Op",
            OPTIONS_ENABLE_HTML2PDF            : "DCBrowserExt:Options:EnableHTML2PDF:ENVIRONMENT:Op",
            HTML_SOURCE_SIZE                   : "DCBrowserExt:HTML:Source:Size:RANGE:Op",
            HTML_SOURCE_SIZE_TOO_LARGE_ERROR   : "DCBrowserExt:HTML:Source:Size:TooLarge:Error:Shown",
            HTML_SOURCE_SITE                   : "DCBrowserExt:HTML:Source:Site:SITE",
            HTML_SOURCE_PROTOCOL               : "DCBrowserExt:HTML:Source:Protocol:PROTOCOL",
            HTML_SOURCE_CONTENT                : "DCBrowserExt:HTML:Source:CONTENT:Op",
            HTML_CONVERSION_STAGE_TIMING       : "DCBrowserExt:HTML:Conversion:STAGE:TIMING",
            OS_MAC_OP                          : "DCBrowserExt:OS:mac:Op",
            OS_WIN_OP                          : "DCBrowserExt:OS:win:Op",
            SHIM_VERSION                       : "DCBrowserExt:Shim:Version:VERSION:Op",
            OPTIONS_PAGE                       : "DCBrowserExt:OptionsPage:Shown",
            FTE_LAUNCH                         : "DCBrowserExt:FTE:Launch:Shown"

        };

        this.event = function (str, params) {
            //don't log analytics if user disabled it from options
            if (!logAnalytics) {
                return;
            }
            if (!str) {
                error("Missing analytics string");
                return;
            }
            params = params || {};
            try {
                this.s.t(formatRequest(this, str, params));
            } catch (e) {
                error(e.toString());
            }
        };

        this.init = function (ver, iType) {
            var cookie = util.getCookie("logAnalytics");
            if (!cookie) {
                logAnalytics = true;
            } else {
                logAnalytics = util.getCookie("logAnalytics") === "true";
            }
            this.version = ver;
            this.installType = iType;
            this.environment = "prod";
            this.s = new AppMeasurement();

            this.s.ssl = true;
            if (this.installType === "development") {
                this.s.account = 'adbcreatepdfplugin.dev';
            } else {
                this.s.account = 'adbcreatepdfplugin.prod';
            }
            this.s.trackingServer = 'stats.adobe.com';
            this.s.trackingServerSecure = 'sstats.adobe.com';
        };

        this.setArg = function (argName, argValue) {
            if (argName) {
                this[argName] = argValue;
            }
        };

        //log multiple analytics at the same time i.e. site, protocol, content
        this.setParamsAndLogAnalytics = function (params, str, key) {
            if (params) {
                var self = this;
                params.forEach(function (c) {
                    self[key] = c;
                    self.event(str);
                });
            }
        };

        this.setAnalyticsUsage = function (useAnl, tabId) {
            logAnalytics = useAnl;
            util.setCookie("logAnalytics", logAnalytics.toString(), 3650);
            util.sendMessage({
                options_op: "saved_analytics",
                tabId: tabId
            });
        };

        this.getAnalyticsUsage = function () {
            return logAnalytics;
        };

        this.setOp = function (op) {
            if (op) {
                this.operation = op;
            }
        };

        this.error = function (params, sev, e) {
            try {
                this.event(this.e.ERROR_SHOWN);
                var error = "DCBrowser:Error:JS:" +
                        e.stack.match(/([A-Za-z0-9\-]+)\.js:(\d*):(\d*)/)[0] +
                        ":" +
                        e.message.replace(/ /g, "_");
                this.event(error);
            } catch (err) {
            }
        };

        if (proxy) {
            // proxy will not be defined at the html client
            proxy.handlers(this.error.bind(this));
        }


        this.checkSizes = function (size) {
            if (size > 0 && size <= 1) {
                this.setArg("size", "0_1");
            } else if (size > 1 && size <= 2) {
                this.setArg("size", "1_2");
            } else if (size > 2 && size <= 5) {
                this.setArg("size", "2_5");
            } else if (size > 5 && size <= 10) {
                this.setArg("size", "5_10");
            } else if (size > 10 && size <= 50) {
                this.setArg("size", "10_50");
            } else if (size > 50 && size <= 500) {
                this.setArg("size", "50_500");
            } else if (size > 500 && size <= 1000) {
                this.setArg("size", "500_1000");
            } else if (size > 1000 && size <= 2000) {
                this.setArg("size", "1000_2000");
            } else if (size > 2000 && size <= 3000) {
                this.setArg("size", "2000_3000");
            } else if (size > 3000 && size <= 4000) {
                this.setArg("size", "3000_4000");
            } else if (size > 4000) {
                this.setArg("size", "4000_");
            }
        };

        this.checkAndLogHTMLBlobSize = function (blobSize) {
            this.checkSizes(blobSize);
            this.event(this.e.HTML_SOURCE_SIZE);
        };

        this.checkAndLogPDFSize = function (pdfSize) {
            this.checkSizes(pdfSize);
            this.event(this.e.PDF_SOURCE_SIZE);
        };

        this.checkAndLogTimingRange = function (timing) {
            if (timing > 0 && timing <= 5) {
                this.setArg("timing", "0_5");
            } else if (timing > 5 && timing <= 10) {
                this.setArg("timing", "5_10");
            } else if (timing > 10 && timing <= 20) {
                this.setArg("timing", "10_20");
            } else if (timing > 20 && timing <= 30) {
                this.setArg("timing", "20_30");
            } else if (timing > 30 && timing <= 50) {
                this.setArg("timing", "30_50");
            } else if (timing > 50 && timing <= 100) {
                this.setArg("timing", "50_100");
            } else if (timing > 100 && timing <= 200) {
                this.setArg("timing", "100_200");
            } else if (timing > 200 && timing <= 600) {
                this.setArg("timing", "200_600");
            } else if (timing > 600 && timing <= 1200) {
                this.setArg("timing", "600_1200");
            } else if (timing > 1200 && timing <= 3000) {
                this.setArg("timing", "1200_3000");
            } else if (timing > 3000) {
                this.setArg("timing", "3000_");
            }
            this.event(this.e.HTML_CONVERSION_STAGE_TIMING);
        };

        this.logSiteAndProtocolAnalytics = function (url) {
            var isIP, isWWW, isTLD, parsedUrl, tld, siteAnalytics = [], protAnalytics = [];

            if (url.startsWith("chrome:")) {
                return;
            }

            parsedUrl = this.parseURL(url);
            isWWW = /^(http|https):\/\/www/.test(url);
            //check simple case of 4 numbers seperated by dots
            isIP = /^(([0-9]+\.){3}([0-9]+))$/.test(url);

            //reverse the hostname array, the last element most prob should be domain name
            tld = parsedUrl.hostname.split(".").reverse()[0].toLowerCase();

            if (isWWW) {
                siteAnalytics.push("WWWW");
            }
            if (isIP) {
                siteAnalytics.push("IP");
            }
            //check for classic top level domain
            if (tld === "com" || tld === "org" || tld === "net" ||
                    tld === "int" || tld === "edu" || tld === "gov" || tld === "mil") {
                siteAnalytics.push("TLD");
            }
            protAnalytics.push(parsedUrl.protocol);

            this.setParamsAndLogAnalytics(
                protAnalytics,
                this.e.HTML_SOURCE_PROTOCOL,
                "protocol"
            );

            this.setParamsAndLogAnalytics(
                siteAnalytics,
                this.e.HTML_SOURCE_SITE,
                "site"
            );
        };

        this.parseURL = function (url) {
            var parser = document.createElement('a');
            parser.href = url;
            return {
                protocol: parser.protocol.slice(0, parser.protocol.length - 1),
                host: parser.host,
                hostname: parser.hostname,
                port: parser.port,
                pathname: parser.pathname
            };
        };


        this.logBrowserAnalytics = function (request) {
            if (request.analytics) {
                request.analytics.forEach(this.proxy(function (a) {
                    this.checkAndLogAnalytics(a);
                }));
                delete request.analytics;
            }
        };

        this.logContents = function (request) {
            if (request.content_analytics) {
                request.content_analytics.forEach(this.proxy(function (a) {
                    this.event(this.e.HTML_SOURCE_CONTENT, {CONTENT: a});
                }));
                delete request.analytics;
            }
        };

        this.checkAndLogAnalytics  = function (key) {
            var found = false, cookie, TC = "TIREKICK", params = {};
            if (!key) {
                error("Missing analytics string");
                return;
            }
            if (key.includes(TC)) {
                cookie = util.getCookie(TC);
                if (cookie) {
                    found = cookie.includes(key);

                    if (found) {
                        params[TC] = "Subsequent";
                    } else {
                        util.setCookie(TC, cookie + "|" + key, 3650);
                        params[TC] = "FirstTime";
                    }
                } else {
                    //first time visit, exp. date about 10 years
                    util.setCookie(TC, key, 3650);
                    params[TC] = "FirstTime";
                }
            }
            this.event(key, params);
        };

        this.logError = function (errorAnalytics) {
            var event = errorAnalytics;
            if (errorAnalytics === "web2pdfHTMLTooLarge") {
                event = this.e.HTML_SOURCE_SIZE_TOO_LARGE_ERROR;
            }
            this.event(event);
        };
    };

    if (!singleton) {
        singleton = new Analytics();
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
    return singleton;
});
