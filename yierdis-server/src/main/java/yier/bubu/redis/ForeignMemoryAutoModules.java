package yier.bubu.redis;

import yier.bubu.redis.args.YierdisCliException;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 当用户显式选择 {@code --offheapBackend foreign} 时，尽量把“需要 --add-modules”的复杂度从用户侧拿走。
 * <p>
 * 说明：Java 17 的 Foreign Memory API 位于 incubator 模块 {@code jdk.incubator.foreign}，默认不会被解析到
 * boot layer。由于模块解析发生在 JVM 启动期，进程内无法“动态补齐”，因此只能通过自动重启追加 JVM 参数来解决。
 */
final class ForeignMemoryAutoModules {
    private static final String FOREIGN_BACKEND = "foreign";
    private static final String FOREIGN_MODULE = "jdk.incubator.foreign";
    private static final String RELAUNCH_MARKER_PROP = "yierdis.foreign.relaunch";

    private ForeignMemoryAutoModules() {
    }

    /**
     * @return 若发生自动重启，返回子进程退出码；否则返回 null（继续当前进程启动流程）。
     */
    static Integer maybeRelaunchIfNeeded(ServerConfig config, String[] appArgs) {
        if (config == null) {
            return null;
        }
        if (!FOREIGN_BACKEND.equalsIgnoreCase(config.offheapBackend)) {
            return null;
        }
        if (isModuleResolvedInBootLayer(FOREIGN_MODULE)) {
            return null;
        }
        if ("1".equals(System.getProperty(RELAUNCH_MARKER_PROP))) {
            return null;
        }
        if (!isModulePresentInJdk(FOREIGN_MODULE)) {
            throw YierdisCliException.userError(
                    "当前 JVM 不包含模块 '" + FOREIGN_MODULE + "'，无法启用 --offheapBackend foreign。"
                            + "请使用 JDK 17 运行，或改用 --offheapBackend unsafe/netty。",
                    null);
        }

        List<String> cmd = buildRelaunchCommand(appArgs);
        System.err.println("检测到 --offheapBackend foreign 且未启用模块 '" + FOREIGN_MODULE + "'，"
                + "将自动重启进程并追加 JVM 参数：--add-modules " + FOREIGN_MODULE);
        try {
            Process child = new ProcessBuilder(cmd)
                    .inheritIO()
                    .start();
            return child.waitFor();
        } catch (IOException e) {
            throw YierdisCliException.userError(
                    "自动追加 JVM 参数失败。请手动使用：java --add-modules " + FOREIGN_MODULE
                            + " -jar <yierdis-server.jar> --offheapBackend foreign ...",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw YierdisCliException.userError(
                    "自动重启被中断。请手动使用：java --add-modules " + FOREIGN_MODULE
                            + " -jar <yierdis-server.jar> --offheapBackend foreign ...",
                    e);
        }
    }

    private static List<String> buildRelaunchCommand(String[] appArgs) {
        List<String> cmd = new ArrayList<>();
        cmd.add(resolveJavaBin());
        cmd.addAll(bestEffortVmArgs());
        cmd.add("-D" + RELAUNCH_MARKER_PROP + "=1");
        cmd.add("--add-modules");
        cmd.add(FOREIGN_MODULE);

        Path jar = currentJarPath();
        if (jar != null) {
            cmd.add("-jar");
            cmd.add(jar.toString());
        } else {
            String cp = System.getProperty("java.class.path");
            if (cp == null || cp.isBlank()) {
                throw YierdisCliException.userError(
                        "无法定位当前 jar/classpath，不能自动重启追加 --add-modules。"
                                + "请手动使用：java --add-modules " + FOREIGN_MODULE
                                + " -cp <classpath> " + YierdisServer.class.getName()
                                + " --offheapBackend foreign ...",
                        null);
            }
            cmd.add("-cp");
            cmd.add(cp);
            cmd.add(YierdisServer.class.getName());
        }

        if (appArgs != null && appArgs.length > 0) {
            cmd.addAll(Arrays.asList(appArgs));
        }
        return cmd;
    }

    private static List<String> bestEffortVmArgs() {
        try {
            RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();
            List<String> in = mxBean.getInputArguments();
            if (in == null || in.isEmpty()) {
                return List.of();
            }
            return new ArrayList<>(in);
        } catch (Throwable ignored) {
            return List.of();
        }
    }

    private static boolean isModuleResolvedInBootLayer(String moduleName) {
        try {
            return ModuleLayer.boot().findModule(moduleName).isPresent();
        } catch (Throwable ignored) {
            return false;
        }
    }

    private static boolean isModulePresentInJdk(String moduleName) {
        try {
            return ModuleFinder.ofSystem().find(moduleName).isPresent();
        } catch (Throwable ignored) {
            return false;
        }
    }

    private static String resolveJavaBin() {
        String javaHome = System.getProperty("java.home");
        if (javaHome != null && !javaHome.isBlank()) {
            Path java = Path.of(javaHome, "bin", "java");
            if (Files.isExecutable(java)) {
                return java.toString();
            }
            Path javaExe = Path.of(javaHome, "bin", "java.exe");
            if (Files.isExecutable(javaExe)) {
                return javaExe.toString();
            }
        }
        return "java";
    }

    private static Path currentJarPath() {
        try {
            CodeSource cs = YierdisServer.class.getProtectionDomain().getCodeSource();
            if (cs == null) {
                return null;
            }
            URL location = cs.getLocation();
            if (location == null) {
                return null;
            }
            Path p = Paths.get(location.toURI()).toAbsolutePath().normalize();
            if (Files.isRegularFile(p) && p.toString().endsWith(".jar")) {
                return p;
            }
            return null;
        } catch (Exception ignored) {
            return null;
        }
    }
}

