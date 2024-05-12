package com.justaddhippopotamus.ghr.server;

import com.google.common.base.Strings;
import com.justaddhippopotamus.ghr.RESP.IRESP;
import com.justaddhippopotamus.ghr.RESP.RESPSimpleError;
import com.justaddhippopotamus.ghr.server.commands.CommandJsonInterpreter;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.logging.Logger;

public class Command {
    /* Most of this stuff is liberally borrowed from version 7.2 of redis */
    public static Command MAKE_CMD(String name,
                                   String summary,
                                   String complexity,
                                   String since,
                                   EnumSet<CommandDocFlags> doc_flags,
                                   String replaced,
                                   String deprecated,
                                   String group,
                                   CommandGroup group_enum,
                                   List<CommandHistory> history,
                                   int num_history,
                                   List<String> tips,
                                   int num_tips,
                                   String function,
                                   int arity,
                                   EnumSet<CommandFlags> flags,
                                   EnumSet<AclCategoryFlags> acl,
                                   List<RedisKeySpec> key_specs,
                                   int key_specs_num,
                                   String get_keys,
                                   int numargs) {
        Command returnValue = new Command();
        returnValue.declared_name = name;
        returnValue.summary = summary;
        returnValue.complexity = complexity;
        returnValue.since = since;
        returnValue.doc_flags = doc_flags;
        returnValue.replaced_by = replaced;
        returnValue.deprecated_since = deprecated;
        returnValue.group = group;
        returnValue.groupEnum = group_enum;
        returnValue.history = history;
        returnValue.tips = tips;
        returnValue.arity = arity;
        returnValue.flags = flags;
        returnValue.acl_categories = acl;
        returnValue.keyParserName = get_keys;
        return returnValue;
    }
    public enum CommandGroup {
        COMMAND_GROUP_GENERIC,
        COMMAND_GROUP_STRING,
        COMMAND_GROUP_LIST,
        COMMAND_GROUP_SET,
        COMMAND_GROUP_SORTED_SET,
        COMMAND_GROUP_HASH,
        COMMAND_GROUP_PUBSUB,
        COMMAND_GROUP_TRANSACTIONS,
        COMMAND_GROUP_CONNECTION,
        COMMAND_GROUP_SERVER,
        COMMAND_GROUP_SCRIPTING,
        COMMAND_GROUP_HYPERLOGLOG,
        COMMAND_GROUP_CLUSTER,
        COMMAND_GROUP_SENTINEL,
        COMMAND_GROUP_GEO,
        COMMAND_GROUP_STREAM,
        COMMAND_GROUP_BITMAP,
        COMMAND_GROUP_MODULE
    }
    public enum RedisCommandArgType {
        ARG_TYPE_STRING,
        ARG_TYPE_INTEGER,
        ARG_TYPE_DOUBLE,
        ARG_TYPE_KEY, /* A string, but represents a keyname */
        ARG_TYPE_PATTERN,
        ARG_TYPE_UNIX_TIME,
        ARG_TYPE_PURE_TOKEN,
        ARG_TYPE_ONEOF, /* Has subargs */
        ARG_TYPE_BLOCK /* Has subargs */
    }

    public enum CommandDocFlags {
        CMD_DOC_NONE,
        CMD_DOC_DEPRECATED,
        CMD_DOC_SYSCMD
    }

    /* Command flags. Please check the definition of struct redisCommand in this file
     * for more information about the meaning of every flag. */
    public enum CommandFlags {
        CMD_WRITE,
        CMD_READONLY,
        CMD_DENYOOM,
        CMD_MODULE,
        CMD_ADMIN,
        CMD_PUBSUB,
        CMD_NOSCRIPT,
        CMD_BLOCKING,
        CMD_LOADING,
        CMD_STALE,
        CMD_SKIP_MONITOR,
        CMD_SKIP_SLOWLOG,
        CMD_ASKING,
        CMD_FAST,
        CMD_NO_AUTH,
        CMD_MAY_REPLICATE,
        CMD_SENTINEL,
        CMD_ONLY_SENTINEL,
        CMD_NO_MANDATORY_KEYS,
        CMD_PROTECTED,
        CMD_MODULE_GETKEYS,
        CMD_MODULE_NOCLUSTER,
        CMD_NO_ASYNC_LOADING,
        CMD_NO_MULTI,
        CMD_MOVABLE_KEYS,
        CMD_ALLOW_BUSY,
        CMD_MODULE_GETCHANNELS,
        CMD_TOUCHES_ARBITRARY_KEYS
    }

    public enum AclCategoryFlags {
        ACL_CATEGORY_KEYSPACE,
        ACL_CATEGORY_READ,
        ACL_CATEGORY_WRITE,
        ACL_CATEGORY_SET,
        ACL_CATEGORY_SORTEDSET,
        ACL_CATEGORY_LIST,
        ACL_CATEGORY_HASH,
        ACL_CATEGORY_STRING,
        ACL_CATEGORY_BITMAP,
        ACL_CATEGORY_HYPERLOGLOG,
        ACL_CATEGORY_GEO,
        ACL_CATEGORY_STREAM,
        ACL_CATEGORY_PUBSUB,
        ACL_CATEGORY_ADMIN,
        ACL_CATEGORY_FAST,
        ACL_CATEGORY_SLOW,
        ACL_CATEGORY_BLOCKING,
        ACL_CATEGORY_DANGEROUS,
        ACL_CATEGORY_CONNECTION,
        ACL_CATEGORY_TRANSACTION,
        ACL_CATEGORY_SCRIPTING
    }

    public enum KeySpecFlags {
        CMD_KEY_RO,
        CMD_KEY_RW,
        CMD_KEY_OW,
        CMD_KEY_RM,
        CMD_KEY_ACCESS,
        CMD_KEY_UPDATE,
        CMD_KEY_INSERT,
        CMD_KEY_DELETE,
        CMD_KEY_NOT_KEY,
        CMD_KEY_INCOMPLETE,
        CMD_KEY_VARIABLE_FLAGS
    }
    public enum CommandArgFlag {
        CMD_ARG_NONE,
        CMD_ARG_OPTIONAL,
        CMD_ARG_MULTIPLE,
        CMD_ARG_MULTIPLE_TOKEN
    }

    enum KeySpecBeginSearchType {
        KSPEC_BS_INVALID,
        KSPEC_BS_UNKNOWN,
        KSPEC_BS_INDEX,
        KSPEC_BS_KEYWORD
    }

    enum KeySpecFindKeyType {
        KSPEC_FK_INVALID,
        KSPEC_FK_UNKNOWN,
        KSPEC_FK_RANGE,
        KSPEC_FK_KEYNUM
    }

    public static class RedisKeySpec {
        public String notes = null;
        public EnumSet<KeySpecFlags> flags = EnumSet.noneOf(KeySpecFlags.class);
        public KeySpecBeginSearchType begin_search_type = KeySpecBeginSearchType.KSPEC_BS_INVALID;
        //Begin Search Parameters (Union in redis server)
        public int pos = 0;
        public String keyword = null;
        public int startfrom = 0;
        public KeySpecFindKeyType find_keys_type = KeySpecFindKeyType.KSPEC_FK_INVALID;
        //Find Keys Parameters (Union in redis server)
        public int lastkey = 0;
        public int keystep = 0;
        public int limit = 0;
        public int keynumindex = 0;
        public int firstkey = 0;

        public RedisKeySpec set_notes(String notes) {
            this.notes = notes;
            return this;
        }

        public RedisKeySpec set_flags(Command.KeySpecFlags...flags) {
            if( flags.length != 0 ) {
                this.flags = EnumSet.noneOf(Command.KeySpecFlags.class);
                for( var a : flags ) {
                    this.flags.add(a);
                }
            }
            return this;
        }

        public RedisKeySpec bs_index(int pos) {
            this.begin_search_type = KeySpecBeginSearchType.KSPEC_BS_INDEX;
            this.pos = pos;
            return this;
        }
        public RedisKeySpec bs_keyword(String keyword, int startfrom ) {
            this.begin_search_type = KeySpecBeginSearchType.KSPEC_BS_KEYWORD;
            this.keyword = keyword;
            this.startfrom = startfrom;
            return this;
        }

        public RedisKeySpec bs_unknown() {
            this.begin_search_type = KeySpecBeginSearchType.KSPEC_BS_UNKNOWN;
            return this;
        }

        public RedisKeySpec fk_range(int lastkey, int keystep, int limit) {
            this.find_keys_type = KeySpecFindKeyType.KSPEC_FK_RANGE;
            this.lastkey = lastkey;
            this.keystep = keystep;
            this.limit = limit;
            return this;
        }

        public RedisKeySpec fk_keynum(int keynumindex, int firstkey, int keystep ) {
            this.find_keys_type = KeySpecFindKeyType.KSPEC_FK_KEYNUM;
            this.keynumindex = keynumindex;
            this.firstkey = firstkey;
            this.keystep = keystep;
            return this;
        }

        public RedisKeySpec fk_unknown() {
            this.find_keys_type = KeySpecFindKeyType.KSPEC_FK_UNKNOWN;
            return this;
        }
    }

    public static class Argument {
        public static Argument MAKE_ARG(String name,
                                 Command.RedisCommandArgType type,
                                 int key_spec_index,
                                 String token,
                                 String summary,
                                 String since,
                                 EnumSet<Command.CommandArgFlag> flags,
                                 int numsubargs,
                                 String deprecated_since) {
            Argument returnValue = new Argument();
            returnValue.name = name;
            returnValue.type = type;
            returnValue.key_spec_index = key_spec_index;
            returnValue.token = token;
            returnValue.summary = summary;
            returnValue.since = since;
            returnValue.flags = flags;
            returnValue.depricated_since = deprecated_since;

            return returnValue;
        }

        public Argument set_display_text(String displayText) {
            this.displayText = displayText;
            return this;
        }

        public Argument set_subargs(List<Argument> subargs) {
            this.subargs = subargs;
            return this;
        }

        public String name = null;
        public Command.RedisCommandArgType type = RedisCommandArgType.ARG_TYPE_INTEGER;
        public int key_spec_index = 0;
        public String token = null;
        public String summary = null;
        public String since = null;
        public EnumSet<Command.CommandArgFlag> flags = EnumSet.noneOf(Command.CommandArgFlag.class);
        public String depricated_since = null;
        public List<Argument> subargs = new ArrayList<>();
        public String displayText = null;
    }

    public static class CommandHistory {
        public String since;
        public String changes;

        public CommandHistory(String since, String changes) {
            this.since = since;
            this.changes = changes;
        }
    }
    @Override
    public String toString() {
        return "Command: " + declared_name;
    }
    //String is lowercase...for some reason?
    public String declared_name;
    public String summary;
    public String complexity;
    public String since;
    public EnumSet<CommandDocFlags> doc_flags = EnumSet.noneOf(CommandDocFlags.class);
    public String replaced_by;
    public String deprecated_since;
    public CommandGroup commandGroup;
    public List<CommandHistory> history;
    public List<String> tips;
    public String group;
    public CommandGroup groupEnum;

    public String implementationName;
    public ICommandImplementation implementation;
    public String keyParserName;
    public ICommandKeyParser keyParser;
    public int arity;
    public EnumSet<CommandFlags> flags = EnumSet.noneOf(CommandFlags.class);
    public EnumSet<AclCategoryFlags> acl_categories = EnumSet.noneOf(AclCategoryFlags.class);
    List<RedisKeySpec> keySpec = new ArrayList<>();
    public Command subcommands(Map<String,Command> subcommands) {
        this.subcommands = subcommands;
        return this;
    }

    public Command args(List<Argument> arguments) {
        this.args = arguments;
        return this;
    }
    public Map<String,Command> subcommands = new HashMap<>();
    public List<Argument> args = new ArrayList<>();

    public Command() {

    }

    private static Map<String,ICommandImplementation> implementationMap = new HashMap<>();
    private static Map<String,ICommandKeyParser> keyParserMap = new HashMap<>();

    private Class<?> getClass(String prefix, String functionName) throws ClassNotFoundException {
        String className = this.getClass().getPackageName() + ".commands.impl." + prefix + functionName.substring(0,1).toUpperCase() + functionName.substring(1);
        Class<?> commandClass = Class.forName(className);
        return commandClass;
    }

    private ICommandKeyParser lookupKeyParser(String functionName) {
        if( functionName == null ) {
            return null;
        }
        if( !keyParserMap.containsKey(functionName) ) {
            try {
                keyParserMap.put(functionName, (ICommandKeyParser)getClass("KeySearch", functionName).getDeclaredConstructor().newInstance());
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                     InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        return keyParserMap.get(functionName);
    }

    private ICommandImplementation lookupCommandImplementation(String functionName) {
        if(Strings.isNullOrEmpty(functionName) ) {
            return null;
        }
        if( !implementationMap.containsKey(functionName) ) {
            try {
                implementationMap.put(functionName, (ICommandImplementation)getClass("Command",functionName).getDeclaredConstructor().newInstance());
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                     InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException("Could not find implementation class for " + functionName,e);
            }
        }
        return implementationMap.get(functionName);
    }

    public Command(CommandJsonInterpreter.Command jsonCommand) {
        this.summary = jsonCommand.summary;
        this.deprecated_since = jsonCommand.deprecated_since;
        this.replaced_by = jsonCommand.replaced_by;
        this.declared_name = jsonCommand.display_name;
        this.implementation = lookupCommandImplementation(jsonCommand.function);
        this.acl_categories = EnumSet.noneOf(AclCategoryFlags.class);
        if (jsonCommand.acl_categories != null && !jsonCommand.acl_categories.isEmpty()) {
            for (String aclCategory : jsonCommand.acl_categories) {
                this.acl_categories.add(AclCategoryFlags.valueOf("ACL_CATEGORY_" + aclCategory));
            }
        }
        this.flags = EnumSet.noneOf(CommandFlags.class);
        if( jsonCommand.command_flags != null ) {
            for (String flag : jsonCommand.command_flags) {
                this.flags.add(CommandFlags.valueOf("CMD_" + flag));
            }
        }
        this.arity = jsonCommand.arity;
        this.keyParser = lookupKeyParser(jsonCommand.get_keys_function);

        if( jsonCommand.subcommands != null ) {
            for( String key : jsonCommand.subcommands.keySet() ) {
                subcommands.put( key, new Command( jsonCommand.subcommands.get(key) ) );
            }
        }
    }
    public static void BadDefaultCommandImplementation(WorkItem item) {
        StringBuilder sb = new StringBuilder();

        sb.append("Go Horse Race does not support ");
        sb.append(item.what);

        item.whoFor.queue(new RESPSimpleError(sb.toString()),item.order);
    }
}
