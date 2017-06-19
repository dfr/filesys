#-
# Copyright (c) 2016-present Doug Rabson
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#

genrule(
    name = "ui",
    srcs = glob([
        "*.html",
        "*.js",
        "*.css",
        "assets/*.svg",
        "components/*/*.html",
        "components/*/*.js",
        "deps/angular/angular.min.js",
        "deps/angular-route/angular-route.min.js",
        "deps/angular-resource/angular-resource.min.js",
        "deps/angular-animate/angular-animate.min.js",
        "deps/angular-aria/angular-aria.min.js",
        "deps/angular-messages/angular-messages.min.js",
        "deps/angular-material/angular-material.min.js",
        "deps/angular-material/angular-material.min.css",
    ]),
    outs = ["ui.h"],
    cmd = "python $(location static-content.py) registerUiContent $(SRCS) > $(OUTS)",
    tools = ["static-content.py"],
    visibility = ["//visibility:public"],
)
