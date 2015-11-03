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

/*global chrome, SETTINGS, $ */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

var util = {
    messageToMain: function (message) {
        'use strict';
        chrome.runtime.sendMessage(message);
    },

    addMainListener: function (handler) {
        'use strict';
        chrome.runtime.onMessage.addListener(handler);
    },

    isFF: function () {
        'use strict';
        return false;
    },

    isChrome: function () {
        'use strict';
        return true;
    },

    consoleLog: function (arg) {
        'use strict';
        if (SETTINGS.DEBUG_MODE) {
            console.log(arg);
        }
    },

    consoleLogDir: function (arg) {
        'use strict';
        if (SETTINGS.DEBUG_MODE) {
            console.dir(arg);
        }
    },

    consoleError: function (arg) {
        'use strict';
        if (SETTINGS.DEBUG_MODE) {
            console.error(arg);
        }
    },

    getTranslation: function (keyStr) {
        'use strict';
        return chrome.i18n.getMessage(keyStr);
    },

    translateElements: function (selector) {
        'use strict';
        $(selector).each(function () {
            if (this.tagName === "INPUT") {
                $(this).val(util.getTranslation(this.id));
            } else {
                $(this).text(util.getTranslation(this.id));
            }
        });
    },

    setCookie: function (cName, cValue, days) {
        'use strict';
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
        'use strict';
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
    }

};

