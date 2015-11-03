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

/*global self, $, define, Blob, FormData, chrome, SETTINGS, atob */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

define(["jquery"], function ($) {
    'use strict';
    return {
        extend: $.extend.bind($),

        isFF: function () {
            return false;
        },

        isChrome: function () {
            return true;
        },

        stackDelimiter: function () {
            return "\n";
        },

        Deferred: $.Deferred.bind($),

        each: $.each.bind($),

        ajax: $.ajax.bind($),

        ajaxError: function (fn) {
            $(document).ajaxError(fn);
        },

        param: $.param.bind($),

        newBlob: function (arg) {
            return new Blob(arg);
        },

        newFormData: function () {
            return new FormData();
        },

        newXHR: function () {
            return new XMLHttpRequest();
        },

        createTab: function (url, callback) {
            if (callback) {
                return chrome.tabs.create({url: url, active: true}, callback);
            }
            return chrome.tabs.create({url: url, active: true});
        },

        isDevEnv: function () {
            var p = $.Deferred();
            chrome.management.getSelf(
                function (env) {
                    p.resolve(env.installType === "development");
                }
            );
            return p.promise();
        },

        closeWindow: function (win) {
            chrome.windows.remove(win.id);
        },

        getTranslation: function (keyStr) {
            return chrome.i18n.getMessage(keyStr);
        },

        sendMessage: function (request, globals) {
            chrome.tabs.sendMessage(request.tabId, request);
        },

        consoleLog: function (arg) {
            if (SETTINGS.DEBUG_MODE) {
                console.log(arg);
            }
        },

        consoleLogDir: function (arg) {
            if (SETTINGS.DEBUG_MODE) {
                console.dir(arg);
            }
        },

        consoleError: function (arg) {
            if (SETTINGS.DEBUG_MODE) {
                console.error(arg);
            }
        },

        setCookie: function (cName, cValue, days) {
            var exp, expDate;
            if (days) {
                expDate = new Date();
                expDate.setDate(expDate.getDate() + days);
                exp = "; expires=" + expDate.toGMTString();
            } else {
                exp = "";
            }
            document.cookie = cName + "=" + cValue + exp + "; path=/";
        },

        getCookie: function (cName) {
            if (document.cookie.length > 0) {
                var parts, cookies = document.cookie;
                parts = cookies.split(cName + "=");
                if (parts.length === 2) {
                    return parts.pop().split(";").shift();
                } else {
                    return "";
                }
            }
            return "";
        },

        atob16: function (b64) {
            // we're receiving a base64 string that was encoded from
            // UTF16 characters
            // atob() converts to UTF8
            // So... combine the UTF8 character pairs to construct UTF16

            var utf8 = atob(b64), out = [], i;
            for (i = 0; i < utf8.length; i += 2) {
                out.push(
                    String.fromCharCode(
                        utf8.charCodeAt(i) + utf8.charCodeAt(i + 1) * 256
                    )
                );
            }

            return out.join("");
        }
    };
});
