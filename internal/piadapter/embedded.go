package piadapter

import _ "embed"

//go:embed session-helper.mjs
var sessionHelper string

//go:embed bot-context-extension.mjs
var botExtension string
