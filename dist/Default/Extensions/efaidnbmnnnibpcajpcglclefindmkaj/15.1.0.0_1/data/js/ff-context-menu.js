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

/*global self, console */
self.on("click", function (node, data) {
    'use strict';
    if (data === "upload-link") {
        self.postMessage({main_op: "do_upload", url: node.href, handleResult: "preview"});
    }
    if (data === "upload-image") {
        self.postMessage({main_op: "do_upload", url: node.src, handleResult: "image_preview"});
    }
    if (data === "upload-page") {
        self.postMessage({main_op: "do_upload", url: node.ownerDocument.URL, handleResult: "preview"});
    }
    if (data === "export") {
        self.postMessage({main_op: "do_upload", url: node.ownerDocument.URL, handleResult: "export"});
    }
});
