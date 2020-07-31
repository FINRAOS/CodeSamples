package picklejar;
import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class Main {
  public static void main(String[] args) {
    System.out.println("java main - enter");
    IFn require = Clojure.var("clojure.core", "require");
    // For using custom Clojure:
    // require.invoke(Clojure.read("picklejar.core"));
    require.invoke(Clojure.read("libpython-clj.require"));
    require.invoke(Clojure.read("libpython-clj.python"));
    IFn require_python = Clojure.var("libpython-clj.require", "require-python");
    require_python.invoke(Clojure.read("numpy"));
    System.out.println("java main - done");
  }
}
