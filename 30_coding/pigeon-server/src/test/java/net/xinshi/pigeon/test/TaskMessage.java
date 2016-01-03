package net.xinshi.pigeon.test;

import net.xinshi.pigeon.adapter.IPigeonEngine;

/**
 * Created with IntelliJ IDEA.
 * User: WPF
 * Date: 13-11-15
 * Time: 下午2:51
 * To change this template use File | Settings | File Templates.
 */
public class TaskMessage implements Comparable {
    String saasName;
    String queueName;
    IPigeonEngine pigeonEngine = null;
    IPigeonEngine rawPigeonEngine = null;

    public String getSaasName() {
        return saasName;
    }

    public void setSaasName(String saasName) {
        this.saasName = saasName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public IPigeonEngine getPigeonEngine() {
        return pigeonEngine;
    }

    public void setPigeonEngine(IPigeonEngine pigeonEngine) {
        this.pigeonEngine = pigeonEngine;
    }

    public IPigeonEngine getRawPigeonEngine() {
        return rawPigeonEngine;
    }

    public void setRawPigeonEngine(IPigeonEngine rawPigeonEngine) {
        this.rawPigeonEngine = rawPigeonEngine;
    }

    public TaskMessage(String saasName, String queueName, IPigeonEngine pigeonEngine, IPigeonEngine rawPigeonEngine) {
        this.saasName = saasName;
        this.queueName = queueName;
        this.pigeonEngine = pigeonEngine;
        this.rawPigeonEngine = rawPigeonEngine;
    }

    public boolean equals(Object object) {
        TaskMessage obj = (TaskMessage) object;
        return (saasName.compareTo(obj.getSaasName()) == 0 && queueName.compareTo(obj.getQueueName()) == 0);
    }

    public int compareTo(Object o) {
        TaskMessage obj = (TaskMessage) o;
        int r = saasName.compareTo(obj.getSaasName());
        if (r != 0) {
            return r;
        }
        return queueName.compareTo(obj.getQueueName());
    }
}
