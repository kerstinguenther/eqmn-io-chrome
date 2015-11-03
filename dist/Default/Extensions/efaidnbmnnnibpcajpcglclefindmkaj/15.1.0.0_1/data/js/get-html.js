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

/*global window, chrome, console, addon, FileReader, Promise, TABID, OP, maxSize, DEBUG, EXCLUDE */
/*jslint devel: true, browser: true, nomen: true */

// Since this method does not use jquery, define a Deferred object
// that simulates $.Deferred using the native javascript promise object
var dc = {
    Deferred: function (promise, timeout) {
        'use strict';
        var This = this,
            p = promise || new Promise(function (res, rej) {
                This.resolve = function () {
                    if (This.timer) {
                        clearTimeout(This.timer);
                    }
                    res();
                };
                This.reject = function () {
                    if (This.timer) {
                        clearTimeout(This.timer);
                    }
                    if (rej) {
                        rej();
                    }
                };
                // if they've asked for a timer, (e.g. when retrieving images)
                // limit the fetch to 60 seconds.
                // This is also our escape in case we've hit an un-caught error
                if (timeout) {
                    This.timer = setTimeout(dc.wrap(function () {
                        This.time_out = true;
                        delete This.timer;
                        This.resolve();
                    }), 60000);
                }
            });
        this.promise = function () {
            return p;
        };
        this.clearTimer = function () {
            if (this.timer) {
                clearTimeout(This.timer);
            }
        };
        this.then = function (fn) {
            return p.then(fn);
        };
        this.done = function (fn) {
            return p.then(fn, fn);
        };
        return this;
    },
    cloneTime: {},
    promises: [],
    URIs: {},
    uriCounter: 0,
    doc_prefix: "",
    analytics: [],
    iframesToProcess: [],
    postData: null,
    topWindow: window.self === window.top,
    output: {
        refs: [],
        origin: location.origin,
        currentSize: 0,
        hasError: false
    },

    wrap: function (func, context) {
        'use strict';
        var run_function = function () {
            var error;
            try {
                return this.func.apply(
                    this.context,
                    this.args.concat(
                        Array.prototype.slice.call(arguments, 0)
                    )
                );
            } catch (e) {
                if (!e.handled) {
                    e.handled = true;
                    // format a string we can use for analytics
                    error = "DCBrowser:Error:JS:" +
                            e.stack.match(/get-html\.js:(\d*):(\d*)/)[0] +
                            ":" +
                            e.message.replace(/\s/g, "_")
                                    // remove anything in quotes.
                                    // could be sensitive data (e.g. URL)
                                     .replace(/"\S*?"/g, '');
                    console.log(error);
                    chrome.runtime.sendMessage(
                        {
                            progress_op: 'html-blob',
                            main_op: OP,
                            error_analytics: error,
                            error: "web2pdfHTMLJSError"
                        }
                    );
                }
                // re-throw so that normal processing is interupted.
                throw e;
            }

        };

        return run_function.bind(
            {
                func: func,
                context: context || {},
                args: Array.prototype.slice.call(arguments, 1)
            }
        );
    },

    random: function () {
        'use strict';
        return "W" + Math.ceil(Math.random() * 100000).toString();
    },

    log: function (msg) {
        'use strict';
        if (DEBUG) {
            console.log(msg);
        }
    },

    getDomain: function (url) {
        'use strict';
        var anchor = document.createElement('a');
        anchor.href = url;
        return anchor.origin;
    },

//nc: might need to change this...
    utf8ByteLength: function (str) {
        'use strict';
        if (!str) { return 0; }
        var escapedStr = encodeURI(str), match = escapedStr.match(/%/g);
        return match ? (escapedStr.length - match.length * 2) : escapedStr.length;
    },

// Take a context URL and a path to a resource and
// construct a URL to the resource
    resolveURL: function (base, url) {
        'use strict';
        var anchor;
        if (!base) {
            return url;
        }
        url = url.trim();
        // prefix relative URLs with the base
        if (url.search(/https?:\/\//) === 0) {
            return url;
        }
        if (url.indexOf("//") === 0) {
            // protocol-relative URL...
            return url;
        }
        if (url.indexOf("/") === 0) {
            // make the url relative to the domain
            return dc.getDomain(base) + url;
        }
        return base + "/" + url;
    },

// allocate a unique, local URI ref.
// If this URL is already resolved, return the previously found entry
// also create a promise that will be resolved once this resource
// has been fetched.
    registerURI: function (base, url, depth) {
        'use strict';

        // the prefix will normally be "refs/"
        // unless the referencing context is already inside refs/
        var prefix = depth === 0 ? "refs/" + dc.doc_prefix : dc.doc_prefix,
            // to get the file suffix, remove query parameters and
            // then look for a 3 or 4 character suffix.
            suffix = "." + url.replace(/[\?#]\S*/, "").split(".").pop();

        if (suffix.length > 5 || suffix.length === 1) {
            suffix = "";
        }

        url = dc.resolveURL(base, url);

        if (dc.URIs[url]) {
            // previously found
            return dc.URIs[url];
        }
        dc.URIs[url] = {
            placeholder: prefix + dc.uriCounter.toString() + suffix
        };
    //    log("MAP: " + url + " to: " + URIs[url].placeholder);
        dc.uriCounter += 1;
        return dc.URIs[url];
    },

    getType: function (ct, placeholder) {
        'use strict';
        var type = "image";


        if (placeholder.endsWith(".ttf")) {
            dc.analytics.push("FONT_TTF");
            type = "font";
        } else if (placeholder.endsWith(".otf")) {
            dc.analytics.push("FONT_OTF");
            type = "font";
        } else if (placeholder.endsWith(".woff")) {
            dc.analytics.push("FONT_WOFF");
            type = "font";
        } else if (placeholder.endsWith(".eot")) {
            dc.analytics.push("FONT_EOT");
            type = "font";
        }

        if (ct === "text/xml" || ct === "application/vnd.ms-fontobject") {
            dc.analytics.push("FONT_EOT");
            type = "font";
        } else if (ct === "font/woff2") {
            dc.analytics.push("FONT_WOFF2");
            type = "font";
        }


        if (ct.startsWith("image/svg")) {
            type = "svg_image";
            dc.analytics.push("SVG_IMAGE");
        }
        return type;
    },
// a callback function to handle a returned image on an ajax request
    readImage: function (xhr, u) {
        'use strict';
        var fr;
        if (xhr.status < 400) {
            u.type = dc.getType(xhr.response.type, u.placeholder);
            // there are certain content types that acrobat can't handle:
            // fonts, svg images.  Save downstream processing by
            // excluding them here.
            if (EXCLUDE.indexOf(u.type) !== -1) {
                u.data = null;
                u.promise.resolve();
            } else {
                fr = new FileReader();
                fr.onloadend = dc.wrap(function (event) {
                    // populate mime type
                    if (!u.promise.time_out) {
                        u.data = event.target.result;
                        u.promise.resolve();
                    }
                });
                fr.readAsDataURL(xhr.response);
            }
        } else {
            // failure to fetch an image may not be our problem.
            // could easily be a problem with the website design
            dc.log("FAILED to load: " + xhr.responseURL);
            dc.log("Placeholder: " + u.placeholder);
            u.promise.resolve();
        }
    },
// fetch an image as a data uri
    getDataURI: function (href, url, depth) {
        'use strict';
        if (url.indexOf("data:") === 0) {
            // already a data uri
            return url;
        }
        if (!url) {
            return url;
        }
        var xhr, u = dc.registerURI(href, url, depth), requestURL;

        if (u.promise) {
            // this image has been resolved previously
            return u.placeholder;
        }
        xhr = new XMLHttpRequest();
        u.promise = new dc.Deferred(null, true);
        dc.promises.push(u.promise);
        requestURL = dc.resolveURL(href, url).replace(/^https?:/, "");
        xhr.open('GET', requestURL, true);
        xhr.responseType = 'blob';
        xhr.onload = dc.wrap(function (e) {
            if (!u.promise.time_out) {
                dc.readImage(this, u);
            }
        }, xhr);
        xhr.send();

        return u.placeholder;
    },

// replace a URL reference in a CSS file
    replaceURL: function (originalURL, localURL, css) {
        'use strict';
        // escape regex characters in the original url name
        var r = new RegExp(originalURL.replace(/([\.\^\$\*\+\?\(\)\[\{\\\|])/g, "\\$1"), "g");
        return css.replace(r, localURL);
    },

    replaceCssRefs: function (href, css, depth) {
        'use strict';
        function findBase(href) {
            var parts = href.split("/");
            if (parts.length < 4) {
                // nothing after the domain portion of the url
                return href;
            }
            return parts.slice(0, -1).join("/");
        }

        // the base is the URL with the last path element removed.
        var base = findBase(href),
            m,
            url,
            urls = [],
            originalURL,
            URL_re = /([\s\S]{0,10})url\s*\(([\s\S]*?)\)/gm;

        // strip out any comments
        css = css.replace(/\/\*[\s\S]*?\*\//gm, "");

        // gather a list of all the URL() references in the CSS contents
        m = URL_re.exec(css);
        while (m) {
            originalURL = m[2];
            url = originalURL.replace(/('|")/g, "");
            // originalURL can be empty...
            if (originalURL && url.indexOf("data:") !== 0) {
                urls.push(
                    {
                        url: url,
                        imprt: m[1].indexOf("@import") !== -1,
                        originalURL: originalURL
                    }
                );
            }
            m = URL_re.exec(css);
        }

        // For the list of all the URL() references, get a local copy
        urls.forEach(dc.wrap(function (u) {
            if (u.imprt) {
                if (OP === 'acro-html') {
                    // until the shim exe is able to handle imported CSS,
                    // replace the css URL with an absolute reference
                    css = dc.replaceURL(u.originalURL, dc.resolveURL(base, u.url), css);
                } else {
                    css = dc.replaceURL(u.originalURL, dc.getCSSDataURI(base, u.url, depth + 1), css);
                }
            } else {
                if (OP === 'acro-html') {
                    // until the shim exe is able to handle background images,
                    // replace the image URL with an absolute reference
                    css = dc.replaceURL(u.originalURL, dc.resolveURL(base, u.url), css);
                } else {
                    css = dc.replaceURL(u.originalURL, dc.getDataURI(base, u.url, depth + 1), css);
                }
            }
        }));

        return css;
    },

    getCSSDataURI: function (base, url, depth) {
        'use strict';
        if (url.indexOf("data:") === 0) {
            // already a data uri
            return url;
        }

        var xhr, u = dc.registerURI(base, url, depth), requestURL;

        if (u.promise) {
            return u.placeholder;
        }
        u.promise = new dc.Deferred(null, true);
        dc.promises.push(u.promise);

        xhr = new XMLHttpRequest();
        // remove the http[s] prefix to ensure that we inherit the
        // protocol of the page. i.e. we cannot make an http
        // request from inside an https URL
        requestURL = dc.resolveURL(base, url).replace(/^https?:/, "");

        xhr.open('GET', requestURL, true);
        xhr.responseType = 'blob';
        xhr.onload = dc.wrap(function (e) {
            var fr;
            if (this.status < 400) {
                if (this.response.type !== "text/css") {
                    dc.readImage(this, u);
                    return;
                }
                fr = new FileReader();
                fr.onloadend = function (event) {
                    u.type = "css";
                    // http request is complete -- cancel timer while we
                    // process nested content.
                    u.promise.clearTimer();
                    if (!u.promise.time_out) {
                        u.data = dc.replaceCssRefs(requestURL, event.target.result, depth);
                        u.promise.resolve();
                    }
                };
                fr.readAsText(this.response);
            } else {
                // failure to fetch a CSS may not be our problem.
                // could easily be a problem with the website design
                // but we should still double check if we see this message in the console
                dc.log("CSS FAILED to load: " + xhr.responseURL + "   Placeholder: " + u.placeholder);
                u.promise.resolve();
            }
        }, xhr);
        xhr.send();
        return u.placeholder;
    },

    processStyleAttr: function (node) {
        'use strict';
        var style = node.getAttribute ? node.getAttribute("style") : "";

        if (style) {
            node.setAttribute("style", dc.replaceCssRefs(document.location.origin, style, -1));
        }
    },

// traverse the cloned DOM, resolving external references
// fetch the resources and replace the links with pointers to local copies
    resolveRefs: function (node, sourceNode) {
        'use strict';
        var i, img, attrs, href, check;
        if (node.tagName === 'LINK' && node.href) {
            check = (node.type || "") + (node.rel || "");
            if (check.includes("icon") || check.includes("image")) {
                dc.log("skipped icon image");
            } else {
                node.href = dc.getCSSDataURI(null, node.href, 0);
            }
        }

        // Eliminate script attributes
        attrs = sourceNode.attributes;
        if (attrs) {
            for (i = 0; i < attrs.length; i += 1) {
                if (attrs[i].name.toLowerCase().indexOf("on") === 0) {
                    node.removeAttribute(attrs[i].name);
                }
            }
        }
        if (["IMG", "INPUT"].indexOf(node.tagName) !== -1) {
            // we used to set the src attribute directly here, but then found
            // that our cloned nodes were trying to resolve those references.
            // It wasn't fatal, but it was ugly in the console.
            // instead we set a data attribute here and rename it later.
            node.dataset._html_to_pdf_src_ = dc.getDataURI(null, node.src, 0);
            node.removeAttribute("src");
        }

        // the value attribute represents the initial value of an input -- not the current value
        // Assign the current value of the sourceNode to the value attribute so that when
        // the cloned HTML renders it will have the correct value.
        if (node.tagName === "INPUT" && node.type !== "file") {
            node.setAttribute("value", sourceNode.value);
        }
        // same for radio and checkbox elements.  We need to make the initial value
        // equal to the current value.
        if (node.tagName === "INPUT" && (node.type === "radio" || node.type === "checkbox")) {
            node.removeAttribute("checked");
            if (sourceNode.checked) {
                node.setAttribute("checked", "checked");
            }
        }
        if (node.tagName === "OPTION") {
            node.removeAttribute("selected");
            if (sourceNode.selected) {
                node.setAttribute("selected", "selected");
            }
        }
        if (node.tagName === "svg") {
            dc.analytics.push("SVG");
        }
        if (node.tagName === "EMBED") {
            if (node.type === "application/x-shockwave-flash") {
                if (dc.topWindow) {
                    dc.analytics.push("FLASH");
                } else {
                    dc.analytics.push("FLASH_IN_IFRAME");
                }
            }
            // likely this is reference to a .swf
            // we're not going to try to include it, because we don't want it
            // to execute on the server.  It could make http requests.
            if (OP === 'html-blob') {
                node.src = "";
            } else if (OP === 'acro-html') {
                node.setAttribute("src", node.src);
            }
        }
        if (node.tagName === "OBJECT") {
            // Most often used to load SWF.
            // TODO: Eventually we could support loading html here by
            // giving it the same treatment as an iframe
            if (node.type === "application/x-shockwave-flash") {
                if (dc.topWindow) {
                    dc.analytics.push("FLASH");
                } else {
                    dc.analytics.push("FLASH_IN_IFRAME");
                }
            }
            if (OP === 'html-blob') {
                node.data = "";
                node.type = "";
            }
        }
        if (node.tagName === 'IFRAME' || node.tagName === 'FRAME') {
            if (OP === 'acro-html') {
                node.setAttribute("src", node.src);
            } else {
                for (i = 0; i < window.frames.length; i += 1) {
                    if (window.frames[i] === sourceNode.contentWindow) {
                        if (sourceNode.contentWindow.WINDOW_ID) {
                            dc.iframesToProcess[i] = {
                                index: i
                            };
                            node.src = sourceNode.contentWindow.WINDOW_ID + ".html";
                        } else {
                            // occasionally we encounter an iframe that doesn't have a WINDOW_ID
                            // that means that somehow it's not visible to the extension.
                            // In this case don't try to process it.
                            node.src = "about:blank";
                        }
                    }
                }
                if (node.name && node.name.length > 256) {
                    // google ads do something very odd where they somehow load document content
                    // from a huge iframe name attribute.
                    node.name = "";
                }
            }
        }
        if (node.tagName === "A") {
            href = node.getAttribute("href");
            if (href && href.indexOf("/") === 0) {
                // at runtime, the href property is turned into an absolute URL
                // Populate the attribute with the absolute URL so that it works in other
                // contexts.  e.g. from a file: protocol.
                node.setAttribute("href", node.href);
            }
        }

        // Check lowercase because that's how style appears inside SVG
        if (node.nodeType === 3 &&
                sourceNode.parentNode &&
                sourceNode.parentNode.tagName.toLowerCase() === "style") {

            node.textContent = dc.replaceCssRefs(document.location.origin, node.textContent, -1);
        }

        if (node.tagName === "CANVAS") {
            img = document.createElement("IMG");
            try {
                img.src = sourceNode.toDataURL("image/png");
            } catch (e) {
                // we can get the dreaded "Tainted canvases may not be exported."
                dc.analytics.push("TAINTED_CANVAS");
                img = document.createElement("DIV");
                img.className = node.className;
            }
            node = img;
            dc.analytics.push("CANVAS");
        }

        if (node.tagName === "image") {
            (function (node) {
                var attrs = node.attributes, n = attrs.length, i, a;
                for (i = 0; i < n; i += 1) {
                    a = attrs[i];
                    // Could be a namespaced attibute. e.g. xlink:href
                    if (a.localName === "href" || a.localName === "src") {
                        node.setAttribute(a.name, dc.getDataURI(null, a.value, -1));
                    }
                }
            }(node));
        }
        dc.processStyleAttr(node);
        return node;
    },

    getDocType: function (doc) {
        'use strict';
        var dt = doc.doctype, str;
        function escape(str) {
            return str.replace(/[&<>]/g, function (s) {
                return {"&": "&amp;", "<": "&lt;", ">": "&gt;"}[s];
            });
        }
        if (dt === null) {
            return "";
        }
        str = "<!DOCTYPE " + dt.name;
        if (dt.publicId) {
            str += ' PUBLIC "' + escape(dt.publicId) + '"';
        } else if (dt.systemId) {
            str += ' SYSTEM';
        }
        if (escape(dt.systemId)) {
            str += ' "' + dt.systemId + '"';
        }
        return str + '>';
    },

// make a copy of the DOM.
// Exclude nodes we don't want/need: scripts, comments
    htmlTree: function (obj) {
        'use strict';

        // reject nodes that we don't need/want to process
        function processNode(node) {
            // display:none
            var display = node.style ? node.style.display : "",
                w,
                h,
                hasSize,
                div;

            if (display.toLowerCase() === "none") {
                return false;
            }

            if (node.nodeType === 8) { // COMMENT_NODE
                return false;
            }

            if (['BASE', 'SCRIPT', 'NOSCRIPT'].indexOf(node.tagName) !== -1) {
                return false;
            }

            if (node.tagName === "LINK" &&
                    node.rel &&
                    node.rel.indexOf("stylesheet") === -1 &&
                    node.rel.indexOf("icon") === -1) {
                return false;
            }

            if (node.tagName === 'IMG') {
                // one pixel elements are normally there for analytics
                if (node.width === 1 && node.height === 1) {
                    return false;
                }
            }

            if (node.tagName === "PARAM") {
                if (node.parentElement.type === "application/x-shockwave-flash") {
                    if (dc.topWindow) {
                        dc.analytics.push("FLASH");
                    } else {
                        dc.analytics.push("FLASH_IN_IFRAME");
                    }
                    if (OP === 'html-blob') {
                        return false;
                    }
                }
            }

            if (node.tagName === 'IFRAME' &&
                    node.src && node.src.indexOf("chrome-extension://") === 0) {
                return false;
            }

            return true;
        }

        var clone, child, finishedNode;
        clone = obj.cloneNode(false);
        clone = dc.resolveRefs(clone, obj);

        if (obj.hasChildNodes()) {
            child = obj.firstChild;
            while (child) {
                if (processNode(child)) {
                    finishedNode = dc.htmlTree(child);
                    clone.appendChild(finishedNode);
                }
                child = child.nextSibling;
            }
        }
        return clone;
    },

// When we traversed the DOM, we collected a set of iframe references
// try to resolve them here
    processIframes: function () {
        'use strict';
        var i, f, msg;

        for (i = 0; i < dc.iframesToProcess.length; i += 1) {
            f = dc.iframesToProcess[i];
            if (f && !f.promise) {
                f.promise = new dc.Deferred();
                dc.promises.push(f.promise);
                msg = {
                    index: i
                };
                chrome.runtime.sendMessage(
                    {
                        main_op: "relay-msg",
                        index: i,
                        frameID: window.frames[i].WINDOW_ID,
                        parentID: window.WINDOW_ID,
                        tabId: TABID
                    }
                );
            }
        }
    },

// a child iframe has completed.
// if it's ours, then incorporate its content.
    receiveIframe: function (msg) {
        'use strict';
        if (msg.html_op !== "serialize_iframe") {
            return;
        }
        if (msg.tabId !== TABID) {
            return;
        }
        if (msg.parentID !== window.WINDOW_ID) {
            // not our iframe
            return;
        }
        dc.analytics = dc.analytics.concat(msg.content_analytics);
        // merge iframe contents with ours
        dc.output.refs = dc.output.refs.concat(msg.refs);
        var size = 0;
        msg.refs.forEach(dc.wrap(function (u) {
            size += u.data ? u.data.length : 0;
        }));
        dc.output.currentSize += size;
        // mark this iframe as complete
        dc.iframesToProcess[msg.index].promise.resolve();
    },

// convert this document (which could be an iframe)
// into stand-alone html with all references resolved.
    _serialize: function (msg) {
        'use strict';
        var html, promiseCount, allLoaded = new dc.Deferred();

        dc.doc_prefix = msg.frameID;

        html = dc.htmlTree(document.documentElement);
        // When all urls have been resolved, send the content over...
        // Note! the list of promises can grow as a result of resolving more external references!
        promiseCount = dc.promises.length;
        function waitForLoadComplete() {
            dc.Deferred.all(dc.promises).done(dc.wrap(function () {
                // once our first round of promises are complete, start
                // processing any iframes
                // We don't do it earlier, because the iframe may not
                // yet be listening for a message from the main frame
                dc.processIframes();
                if (dc.promises.length === promiseCount) {
                    allLoaded.resolve();
                } else {
                    promiseCount = dc.promises.length;
                    waitForLoadComplete();
                }
            }));
            return allLoaded.promise();
        }

        waitForLoadComplete().then(dc.wrap(function () {
            var u, err, uniqueAnalytics, tm, end;

            // rename the src attributes back.
            // From here on there's no danger that they'd get resolved.
            dc.output.html = dc.getDocType(document) +
                    html.outerHTML.replace(/data\-_html_to_pdf_src_=/gm, "src=");
            // convert extended characters to entity references.
            // (easier than trying to find the various places where extended characters
            // get lost in transmission)
            dc.output.html = dc.output.html.replace(/[\u00A0-\uFFFF]/g, function (c) {
                return '&#' + c.charCodeAt(0) + ';';
            });
            dc.output.currentSize += dc.output.html.length;

            for (u in dc.URIs) {
                if (dc.URIs.hasOwnProperty(u)) {
                    //check for the size limit! 1024 conversion
                    if (dc.output.currentSize > (maxSize * 1048576)) {
                        err = 'web2pdfHTMLTooLarge';
                        dc.output.hasError = true;
                        break;
                    } else {
                        dc.output.refs.push(
                            {
                                placeholder: dc.URIs[u].placeholder,
                                type: dc.URIs[u].type,
                                data: dc.URIs[u].data
                            }
                        );
                        if (dc.URIs[u].data) {
                            dc.output.currentSize += dc.URIs[u].data.length;
                        }
                    }
                }
            }

            uniqueAnalytics = dc.analytics.filter(function (itm, i, a) {
                return i === dc.analytics.indexOf(itm);
            });

            if (dc.topWindow) {
                //calculate how long it took to clone
                //time in 1/10 seconds
                end = new Date();
                tm = (end.getTime() - dc.cloneTime.start) / 100;
                chrome.extension.sendMessage(
                    {
                        progress_op: 'html-blob',
                        main_op: OP,
                        blob: dc.output,
                        content_analytics: uniqueAnalytics,
                        cloneTiming: tm,
                        error: err,
                        error_analytics: err
                    }
                );
                dc = null;
            } else {
                // in this case we're an iframe and are sending content
                // back to the extension background -- which will, in turn,
                // send it back to the parent document via receiveIframe()
                dc.output.refs.push({
                    placeholder: window.WINDOW_ID + ".html",
                    type: "html",
                    data: dc.output.html
                });
                dc.output.currentSize += dc.output.html.length;

                //check for the size limit! 1024 conversion from Mbytes to Bytes
                if (dc.output.currentSize > (maxSize * 1048576)) {
                    err = "web2pdfHTMLTooLarge";
                }
                //calculate how long it took to clone
                //time in 1/10 seconds
                end = new Date();
                tm = (end.getTime() - dc.cloneTime.start) / 100; //time in 1/10 seconds

                chrome.runtime.sendMessage(
                    {
                        tabid: TABID,
                        main_op: "relay-msg",
                        complete: true,
                        index: msg.index,
                        refs: dc.output.refs,
                        frameID: window.WINDOW_ID,
                        parentID: msg.parentID,
                        content_analytics: uniqueAnalytics,
                        cloneTiming: tm,
                        error: err,
                        error_analytics: err
                    }
                );
                // free memory for gc
                dc = null;
            }
        }));
    }
};

// method to wait on an array of promises
dc.Deferred.all = function (array) {
    'use strict';
    return new dc.Deferred(Promise.all(array));
};
window.WINDOW_ID = dc.random();


function receiveIframe(msg) {
    'use strict';
    // dc could be null if we've already processed this iframe
    dc.wrap(dc.receiveIframe)(msg);
}


function serialize(msg) {
    'use strict';
    var winid = msg.frameID;
    if (winid !== window.WINDOW_ID) {
        return;
    }
    window.setTimeout(dc.wrap(dc._serialize), 0, msg);
}

if (dc.topWindow) {
    // we're the top window, initiate the conversion here.
    dc.cloneTime.start = new Date();
    serialize({frameID: window.WINDOW_ID});
}
