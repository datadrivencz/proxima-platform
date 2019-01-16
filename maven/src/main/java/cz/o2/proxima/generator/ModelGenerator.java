/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.generator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.CamelCase;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Generates code for accessing data of entity and it's attributes.
 */
@Slf4j
public class ModelGenerator {

  private final String javaPackage;
  private final String className;
  private final File sourceConfigPath;
  private final File outputPath;

  public ModelGenerator(
      String javaPackage, String className,
      String sourceConfigPath, String outputPath) {

    this(javaPackage, className, sourceConfigPath, outputPath, true);
  }

  ModelGenerator(
      String javaPackage, String className,
      String sourceConfigPath, String outputPath,
      boolean validate) {


    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(javaPackage),
        "Java package name is missing");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(className),
        "Class name is missing");

    this.javaPackage = javaPackage;
    this.className = className;
    this.sourceConfigPath = new File(sourceConfigPath);
    this.outputPath = new File(outputPath);

    if (validate) {
      if (!this.sourceConfigPath.exists()) {
        throw new IllegalArgumentException(
            "Source config not found at [ " + sourceConfigPath + " ]");
      }

      if (!this.outputPath.isAbsolute()) {
        throw new IllegalArgumentException(
            "Output path must be absolute [ " + outputPath + " ]");
      }
    }
  }

  public void generate() throws Exception {
    File output = getOutputDirForPackage(outputPath, javaPackage);
    final File outputFile = new File(output, className + ".java");
    if (!output.exists() && !output.mkdirs()) {
      throw new RuntimeException(
          "Failed to create directories for [ " + outputPath.getAbsolutePath() + " ]");
    }

    try (FileOutputStream out = new FileOutputStream(outputFile)) {
      generate(
          ConfigFactory.parseFile(sourceConfigPath).resolve(),
          new OutputStreamWriter(out));
    }
  }

  @VisibleForTesting
  void generate(Config config, Writer writer)
      throws IOException, TemplateException {

    final Configuration conf = getConf();

    final Repository repo = ConfigRepository.Builder
        .of(config)
        .withReadOnly(true)
        .withValidate(false)
        .withLoadFamilies(false)
        .withLoadClasses(false)
        .build();

    Map<String, Object> root = new HashMap<>();

    List<OperatorGenerator> operatorGenerators = getOperatorGenerators(repo);

    final Set<String> operatorImports = operatorGenerators
        .stream()
        .map(OperatorGenerator::imports)
        .reduce(Sets.newHashSet(), Sets::union);

    final List<Map<String, String>> operators = operatorGenerators
        .stream()
        .map(this::toOperatorSubclassDef)
        .collect(Collectors.toList());

    final List<Map<String, Object>> entities = getEntities(repo);

    root.put("input_path", sourceConfigPath.getAbsoluteFile());
    root.put("input_config", readFileToString(sourceConfigPath));
    root.put("java_package", javaPackage);
    root.put("java_classname", className);
    root.put("java_config_resourcename", sourceConfigPath.getName());
    root.put("entities", entities);
    root.put("imports", operatorImports);
    root.put("operators", operators);
    Template template = conf.getTemplate("java-source.ftlh");
    template.process(root, writer);
  }

  private Configuration getConf() {
    Configuration conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
    return conf;
  }

  private File getOutputDirForPackage(File outputPath, String javaPackage) {
    String packagePath = javaPackage.replaceAll("\\.", File.separator);
    return new File(outputPath, packagePath);
  }

  private List<Map<String, Object>> getEntities(Repository repo) {
    List<Map<String, Object>> ret = new ArrayList<>();
    repo.getAllEntities().forEach(e -> ret.add(getEntityDict(e)));
    return ret;
  }

  private Map<String, Object> getEntityDict(EntityDescriptor e) {
    Map<String, Object> ret = new HashMap<>();
    ret.put("classname", toClassName(e.getName()));
    ret.put("name", e.getName());
    ret.put("nameCamel", CamelCase.apply(e.getName()));

    List<Map<String, Object>> attributes = e.getAllAttributes().stream()
        .map(attr -> {
          Map<String, Object> attrMap = new HashMap<>();
          String nameModified = attr.toAttributePrefix(false);
          attrMap.put("wildcard", attr.isWildcard());
          attrMap.put("nameRaw", attr.getName());
          attrMap.put("name", nameModified);
          attrMap.put("nameCamel", CamelCase.apply(nameModified));
          attrMap.put("nameUpper", nameModified.toUpperCase());
          // FIXME: this is working just for protobufs
          attrMap.put("type", attr.getSchemeUri().getSchemeSpecificPart());
          return attrMap;
        })
        .collect(Collectors.toList());
    ret.put("attributes", attributes);
    return ret;
  }

  private String toClassName(String name) {
    return CamelCase.apply(name);
  }

  private String readFileToString(File path) throws IOException {
    try {
      return Joiner.on("\n + ").join(
          IOUtils.readLines(new FileInputStream(path), "UTF-8")
              .stream()
              .map(s -> "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\\n\"")
              .collect(Collectors.toList()));
    } catch (IOException ex) {
      log.warn("Failed to read file {}. Ignoring.", path, ex);
      return "FAILED: " + ex.getMessage();
    }
  }

  private List<OperatorGenerator> getOperatorGenerators(Repository repo) {
    List<OperatorGenerator> ret = new ArrayList<>();
    ServiceLoader<OperatorGeneratorFactory> loader = ServiceLoader.load(
        OperatorGeneratorFactory.class);
    for (OperatorGeneratorFactory ogf : loader) {
      ret.add(ogf.create(repo));
    }
    return ret;
  }

  private Map<String, String> toOperatorSubclassDef(OperatorGenerator generator) {
    Map<String, String> ret = new HashMap<>();
    ret.put("operatorClass", generator.getOperatorClassName());
    ret.put("classdef", generator.classDef());
    ret.put("name", generator.operatorFactory().getOperatorName());
    ret.put("classname", toClassName(
        generator.operatorFactory().getOperatorName() + " operator"));

    return ret;
  }

}
