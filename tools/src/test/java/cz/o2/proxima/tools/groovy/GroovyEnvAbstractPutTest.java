package cz.o2.proxima.tools.groovy;

import com.typesafe.config.Config;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.junit.Before;

import static org.junit.Assert.fail;

public abstract class GroovyEnvAbstractPutTest {

  static Repository repo;
  static Console console;
  static Config cfg;

  DirectDataOperator direct;
  Configuration conf;
  GroovyClassLoader loader;

  @Before
  public void setUp() {
    console = Console.create(cfg, repo);
    direct = console.getDirectDataOperator();
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);

    loader = new GroovyClassLoader(Thread.currentThread().getContextClassLoader());
    Thread.currentThread().setContextClassLoader(loader);
  }


  void executeTest(AbstractTest test, AttributeDescriptor attr) throws Exception {
    test.execute(
        Optionals.get(direct.getCommitLogReader(attr)));
  }


  @SuppressWarnings("unchecked")
  Script compile(String script) throws Exception { // @TODO: refactor
    String source = GroovyEnv.getSource(conf, repo)
        + "\n"
        + "env = cz.o2.proxima.tools.groovy.Console.get().getEnv()"
        + "\n"
        + script;
    Class<Script> parsed = loader.parseClass(source);
    return parsed.newInstance();
  }


  protected abstract static class AbstractTest {

    abstract Script inputScript() throws Exception;

    abstract void validate(StreamElement element);

    void execute(CommitLogReader commitLog) throws Exception {
      commitLog.observe("test-observer", new LogObserver() {
        @Override
        public boolean onError(Throwable error) {
          fail(error.getMessage());
          return false;
        }

        @Override
        public boolean onNext(StreamElement ingest, OnNextContext context) {
          validate(ingest);
          return false;
        }
      });
      inputScript().run();
    }

  }

}
