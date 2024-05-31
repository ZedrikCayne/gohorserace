Add an issue should you find something terribly, awfully wrong and insecure
about gohorserace. The LUA library doesn't provide access to Java or the
local file system as configured, and otherwise the system is pretty self
contained in its own ecosystem. That being said, the lua system is pretty
wide open and programmable and there's only a fig leaf of a password system
that isn't on by default.

Do not expose this to the wide internet as you would not for any other redis
system.
