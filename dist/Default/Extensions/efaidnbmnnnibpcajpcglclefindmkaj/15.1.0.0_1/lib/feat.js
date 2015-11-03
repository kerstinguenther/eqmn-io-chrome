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

/*global window, $, console, util, chrome, SETTINGS, require */
/*jslint devel: true, browser: true, nomen: true, maxlen: 80 */

// Handle FEAT test requests from a companion extension

var communicate = require("communicate");

chrome.extension.onMessageExternal.addListener(
    function (req, sender, sendResponse) {
        'use strict';

        if (req.type !== "Automation") {
            return;
        }
        // our white-listed FEAT extension
        if (sender.id !== "bngnhmnppadfcmpggglniifohlkmddfc") {
            return;
        }

        chrome.tabs.sendMessage(
            communicate.activeTab(),
            {
                panel_op        : "test",
                test_extension  : 'to_html',
                outputPath      : req.path
            }
        );
    }
);


