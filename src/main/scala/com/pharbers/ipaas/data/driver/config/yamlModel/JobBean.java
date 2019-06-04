package com.pharbers.ipaas.data.driver.config.yamlModel;

import java.util.List;

public class JobBean {
    private String name;
    private String reference;
    private String factory;
    private List<ActionBean> actions;

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

    public List<ActionBean> getActions() {
        return actions;
    }

    public void setActions(List<ActionBean> actions) {
        this.actions = actions;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }
}
