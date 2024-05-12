package com.justaddhippopotamus.ghr.server.commands;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandJsonInterpreter {
    public static class Argument {
        public String name;
        public String type;
        public Boolean multiple;
        public Integer key_spec_index;
        public String token;
        public Boolean optional;
        public String depricated_since;
        public String display_text;
        public List<Argument> arguments;
    }
    public static class KeySpec {
        public List<String> flags;
        public Map<String,Map<String,String>> begin_search;
        public Map<String,Map<String,String>> find_keys;
    }

    public static class Command {
        public String display_name;
        public String summary;
        public String complexity;
        public String group;
        public String since;
        public Integer arity;
        public String deprecated_since;
        public String replaced_by;
        public String container;
        public String function;
        public String get_keys_function;
        public List<List<String>> history;
        public List<String> command_flags;
        public List<String> acl_categories;
        public List<KeySpec> key_specs;
        public List<Argument> arguments;
        public Map<String,Command> subcommands;
    }

    public Map<String,Command> root = new HashMap<String,Command>();
    public List<Command> subCommands = new ArrayList<Command>();

    public void loadNew( InputStream input ) {
        Gson gson = new GsonBuilder().setLenient().create();
        Map<String,Command> jsonIn = gson.fromJson(new InputStreamReader(input),new TypeToken<Map<String,Command>>(){}.getType());
        for( String key : jsonIn.keySet() ) {
            Command commandIn = jsonIn.get(key);
            commandIn.display_name = key.toLowerCase();
            if( Strings.isNullOrEmpty(commandIn.container) ) {
                commandIn.subcommands = new HashMap<>();
                root.put(key,commandIn);
            } else {
                subCommands.add(commandIn);
            }
        }
    }

    public void linkSubcommands() {
        for( Command sub : subCommands ) {
            root.get(sub.container).subcommands.put(sub.display_name.toUpperCase(),sub);
        }
    }


}
