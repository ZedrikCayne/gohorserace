grep -l BadDefaultCommandImplementation ../app/src/main/java/com/justaddhippopotamus/ghr/server/commands/impl/Command*.java | sed 's/^.*\/Command//' | sed 's/Command\.java//' | tr "[a-z]" "[A-Z]"
