package net.xinshi.pigeon.tags;

import net.xinshi.pigeon.tags.utils.ConfigHelper;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.tagext.Tag;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: mac
 * Date: 12-1-21
 * Time: 下午3:23
 * To change this template use File | Settings | File Templates.
 */
public class UrlTag implements Tag {
    PageContext pageContext;
    Tag parent;
    String value;
    String var;
    String domain;
    String scope;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getVar() {
        return var;
    }

    public void setVar(String var) {
        this.var = var;
    }

    @Override
    public void setPageContext(PageContext pageContext) {
       this.pageContext = pageContext;
    }

    @Override
    public void setParent(Tag tag) {
       this.parent = tag;
    }

    @Override
    public Tag getParent() {
        return parent;
    }


    private void outputValue(String value) throws JspException {
        if(var==null){
            JspWriter writer = pageContext.getOut();
            try {
                writer.write(value);
            } catch (IOException e) {
                throw new JspException(e.getMessage()) ;
            }
        }
        else{
            if(scope==null){
               pageContext.setAttribute(var,value);
            }
            else{
                scope =scope.toLowerCase();
                if(scope.equals("page")){
                    pageContext.setAttribute(var,value,PageContext.PAGE_SCOPE);
                }
                else if(scope.equals("request")){
                    pageContext.setAttribute(var,value,PageContext.REQUEST_SCOPE);
                }
                else if(scope.equals("session")){
                    pageContext.setAttribute(var,value,PageContext.SESSION_SCOPE);
                }
                else{
                    pageContext.setAttribute(var,value);
                }
            }
        }
    }
    @Override
    public int doStartTag() throws JspException {

        if(value.startsWith("http")){
            outputValue(value);
            return SKIP_BODY;
        }

        String context = pageContext.getServletContext().getContextPath();





        if(!value.startsWith("/")){
           outputValue(value);
            return SKIP_BODY;
        }

        String key = domain;
        if(key==null){
            key = "defaultDomain";
        }
        String prefix = ConfigHelper.getConfigProperty(key);
        if(prefix!=null)  {
            if(prefix.endsWith("/")){
                prefix = prefix.substring(0,prefix.length() - 1);
            }
            if(value.startsWith("/")){
                value = value.substring(0,value.length());
            }
            value = prefix + value;
        }
        else{
            value=context + value;
        }
        outputValue(value);

        return SKIP_BODY;
    }

    @Override
    public int doEndTag() throws JspException {
        return 0;
    }

    @Override
    public void release() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
