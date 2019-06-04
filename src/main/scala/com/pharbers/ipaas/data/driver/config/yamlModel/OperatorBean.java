package com.pharbers.ipaas.data.driver.config.yamlModel;

import java.util.List;
import java.util.Map;

public class OperatorBean {
    private String name;
    private String reference;
    private String factory;
    private Map<String, String> args;
    private PluginBean plugin;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFactory() {
        return factory;
    }

    public void setFactory(String factory) {
        this.factory = factory;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

    public PluginBean getPlugin() {
        return plugin;
    }

    public void setPlugin(PluginBean plugin) {
        this.plugin = plugin;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }
}
