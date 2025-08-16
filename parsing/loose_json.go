package parsing
import (
    "strings"
    "strconv"
    "regexp"
    "unicode"
)

var NUM_RGX = regexp.MustCompile(`^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+\-]?\d+)?$`)

type ctx struct {
    // '{' or '['
    kind         byte 
    // only meaningful for '{'
    expectingKey bool
}

// This function parses a JSON superset where strings can
// be unquoted. This is used for CLI in parameter substitution,
// for strings like -p Start.fastqDir=/path1/path2/path3. In
// general, we expect everything following the "=" token in
// such strings to be a JSON-parsable value, but having to
// quote everything would be cumbersome.
func NormalizeLooseJSON(in string) (string, error) {
    var out strings.Builder
    var stack []ctx
    var tok strings.Builder

    top := func() *ctx {
        if len(stack) == 0 {
            return nil
        }
        return &stack[len(stack)-1]
    }
    inKey := func() bool {
        t := top()
        return t != nil && t.kind == '{' && t.expectingKey
    }

    flush := func() error {
        if tok.Len() == 0 {
            return nil
        }
        s := tok.String()
        tok.Reset()

        if inKey() {
            out.WriteString(strconv.Quote(s))
            return nil
        }

        // value position
        switch s {
        case "true", "false", "null":
            out.WriteString(s)
            return nil
        }
        if NUM_RGX.MatchString(s) {
            out.WriteString(s)
            return nil
        }
        out.WriteString(strconv.Quote(s))
        return nil
    }

    i := 0
    n := len(in)
    for i < n {
        c := in[i]

        // whitespace -> boundary
        if unicode.IsSpace(rune(c)) {
            if err := flush(); err != nil {
                return "", err
            }
            i++
            continue
        }

        // string literal: copy as-is
        if c == '"' {
            if err := flush(); err != nil {
                return "", err
            }
            out.WriteByte('"')
            i++
            for i < n {
                ch := in[i]
                out.WriteByte(ch)
                i++
                if ch == '\\' { // escape next
                    if i < n {
                        out.WriteByte(in[i])
                        i++
                    }
                    continue
                }
                if ch == '"' {
                    break
                }
            }
            continue
        }

        // structural characters
        switch c {
        case '{':
            if err := flush(); err != nil {
                return "", err
            }
            out.WriteByte('{')
            stack = append(stack, ctx{kind: '{', expectingKey: true})
            i++
            continue
        case '}':
            if err := flush(); err != nil {
                return "", err
            }
            out.WriteByte('}')
            if t := top(); t != nil {
                stack = stack[:len(stack)-1]
            }
            i++
            continue
        case '[':
            if err := flush(); err != nil {
                return "", err
            }
            out.WriteByte('[')
            stack = append(stack, ctx{kind: '['})
            i++
            continue
        case ']':
            if err := flush(); err != nil {
                return "", err
            }
            out.WriteByte(']')
            if t := top(); t != nil {
                stack = stack[:len(stack)-1]
            }
            i++
            continue
        case ',':
            if err := flush(); err != nil {
                return "", err
            }
            out.WriteByte(',')
            if t := top(); t != nil && t.kind == '{' {
                t.expectingKey = true
            }
            i++
            continue
        case ':':
            // It's a key/value separator ONLY if we're currently expecting a value after a key.
            if inKey() {
                if err := flush(); err != nil {
                    return "", err
                }
                out.WriteByte(':')
                top().expectingKey = false
                i++
                continue
            }
            // Otherwise ':' is part of a value token (e.g. "http://..."), fall through and treat as token char.
        }

        // token character (part of bareword). We include '/', '\', '.', '-', '_', etc.
        tok.WriteByte(c)
        i++
    }

    if err := flush(); err != nil {
        return "", err
    }
    return out.String(), nil
}