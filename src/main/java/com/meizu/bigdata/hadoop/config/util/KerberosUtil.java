package com.meizu.bigdata.hadoop.config.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.meizu.bigdata.hadoop.config.Key;
import com.meizu.bigdata.hadoop.config.Location;
import com.meizu.bigdata.hadoop.config.auth.KerberosUser;
import com.meizu.bigdata.hadoop.config.auth.PasswordCallback;

public final class KerberosUtil {

    private static final Log   LOG           = LogFactory.getLog(KerberosUtil.class);

    public final static String DEFAULT_REALM = "MEIZU.COM";
    public final static String NAME          = "krb5.conf";
    public final static String JAR_CONF_PATH = "config/" + NAME;
    public final static String TMP_DIR       = System.getProperty("java.io.tmpdir");


    private KerberosUtil() {
    }

    final static URL JAR_INNER_URL = Location.class.getClassLoader().getResource(JAR_CONF_PATH);


    /**
     * 把jar包内的krb5.conf配置文件 转存到tmp目录
     * 
     * @return
     */
    public static String getDefaultKrb5ConfResouce() {
        StringBuffer tmp = new StringBuffer(TMP_DIR);
        tmp.append(File.separatorChar).append(NAME);
        File tmpconf = new File(tmp.toString());

        try {
            if (!tmpconf.exists()) {
                tmpconf.createNewFile();
            }

            InputStream in = JAR_INNER_URL.openStream();
            FileOutputStream fos = new FileOutputStream(tmpconf);
            byte[] b = new byte[1024 * 2];
            int len = -1;
            while ((len = in.read(b)) > -1) {
                fos.write(b, 0, len);
            }
            in.close();
            fos.flush();
            fos.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return tmpconf.getPath();
    }


    /**
     * kerberos 认证登录
     * 
     * @param username
     * @param call
     * @return
     */
    public synchronized static Subject login(String username, PasswordCallback<KerberosUser> call) {
        KerberosUser usr = call.supply(username);
        if (usr == null) {
            throw new RuntimeException("get password exception.");
        }
        return login(usr);
    }


    /**
     * 安全登录到 Kerberos
     *
     * @param username
     * @param password
     * @return
     * @throws LoginException
     */
    public synchronized static Subject login(final KerberosUser usr) {
        try {
            LOG.warn(String.format("begin login kerberos[%s]", usr));

            LOG.warn(String.format("KDC[%s]", System.getProperty(Key.JAVA_SECURITY_KRB5_KDC)));
            LOG.warn(String.format("REALM[%s]", System.getProperty(Key.JAVA_SECURITY_KRB5_REALM)));
            LOG.warn(String.format("CONF[%s]", System.getProperty(Key.JAVA_SECURITY_KRB5_CONF)));

            Set<Principal> princ = new HashSet<Principal>(1);
            princ.add(new KerberosPrincipal(usr.getUsername()));

            Subject sub = new Subject(false, princ, new HashSet<Object>(), new HashSet<Object>());

            LoginContext lc = new LoginContext("auth-kerberos-login", sub,
                    new javax.security.auth.callback.CallbackHandler() {
                        public void handle(javax.security.auth.callback.Callback[] callbacks)
                                throws IOException, UnsupportedCallbackException {

                            for (javax.security.auth.callback.Callback c : callbacks) {
                                if (c instanceof javax.security.auth.callback.NameCallback) {
                                    ((javax.security.auth.callback.NameCallback) c)
                                            .setName(usr.getUsername());

                                } else if (c instanceof javax.security.auth.callback.PasswordCallback) {
                                    ((javax.security.auth.callback.PasswordCallback) c)
                                            .setPassword(usr.getPassword().toCharArray());
                                }
                            }
                        }
                    }, createJaasConfiguration());

            lc.login();
            return lc.getSubject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * 使用LoginContext的接口进行登录验证。LoginContext可以配置使用不同的验证协议。验证通过后，用户得到一个subject，
     * 里面包含凭证，公私钥等。之后，在涉及到需要进行权限认证的地方（例如，资源访问，外部链接校验，协议访问等）， 使用doAs函数()代替直接执行。
     * 
     * @return
     */
    private static javax.security.auth.login.Configuration createJaasConfiguration() {

        return new javax.security.auth.login.Configuration() {

            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, Object> options = new HashMap<String, Object>();
                options.put("renewTGT", "false");
                options.put("useKeyTab", "false");
                options.put("refreshKrb5Config", "false");

                // options.put("debug", "true");

                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
            }
        };
    }
}
