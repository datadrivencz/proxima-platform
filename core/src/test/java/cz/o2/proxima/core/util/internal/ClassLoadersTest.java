/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.util.internal;

import static junit.framework.Assert.assertSame;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cz.o2.proxima.core.util.internal.ClassLoaders.ChildFirstURLClassLoader;
import cz.o2.proxima.core.util.internal.ClassLoaders.ChildLayerFirstClassLoader;
import java.io.IOException;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Test;

public class ClassLoadersTest {

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testChildFirstURLClassLoaderInParent() throws ClassNotFoundException {
    ChildLayerFirstClassLoader mockParent = mock(ChildLayerFirstClassLoader.class);
    Class<?> res = Integer.class;
    when(mockParent.loadClass(any(), anyBoolean())).thenReturn((Class) res);
    ChildFirstURLClassLoader loader = new ChildFirstURLClassLoader(new URL[] {}, mockParent);
    Class<?> clazz = loader.loadClass("test", true);
    assertEquals(res, clazz);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testChildFirstURLClassLoaderInChild() throws ClassNotFoundException {
    ChildLayerFirstClassLoader mockParent = mock(ChildLayerFirstClassLoader.class);
    Class<?> parent = Integer.class;
    Class<?> child = Long.class;
    when(mockParent.loadClass(any(), anyBoolean())).thenReturn((Class) parent);
    ChildFirstURLClassLoader loader =
        new ChildFirstURLClassLoader(new URL[] {}, mockParent) {
          @Override
          protected Class<?> findClass(String name) {
            return child;
          }
        };
    Class<?> clazz = loader.loadClass("test", true);
    assertEquals(child, clazz);
  }

  @Test
  public void testChildFirstURLClassLoaderInSibling() throws ClassNotFoundException {
    Class<?> sibling = String.class;
    AtomicReference<ChildFirstURLClassLoader> moduleALoader = new AtomicReference<>();
    AtomicReference<ChildFirstURLClassLoader> moduleBLoader = new AtomicReference<>();
    ModuleFinder finder = mock(ModuleFinder.class);
    ChildLayerFirstClassLoader parentLoader =
        new ChildLayerFirstClassLoader(finder, Thread.currentThread().getContextClassLoader()) {

          @Override
          Stream<String> getNamesForModules(ModuleFinder finder) {
            return Arrays.asList("module.a", "module.b").stream();
          }

          @Override
          ChildFirstURLClassLoader getModuleLoader(String module) {
            if (module.equals("module.a")) {
              return moduleALoader.get();
            } else if (module.equals("module.b")) {
              return moduleBLoader.get();
            }
            throw new IllegalArgumentException(module);
          }
        };

    parentLoader.setLayer(mock(ModuleLayer.class));
    moduleALoader.set(new ChildFirstURLClassLoader(new URL[] {}, parentLoader));
    moduleBLoader.set(
        new ChildFirstURLClassLoader(new URL[] {}, parentLoader) {
          @Override
          protected Class<?> findClass(String name) {
            return sibling;
          }
        });

    Class<?> clazz = moduleALoader.get().loadClass("test", true);
    assertEquals(sibling, clazz);
  }

  @Test
  public void testFindResources() throws ClassNotFoundException, IOException {
    ModuleFinder finder = mock(ModuleFinder.class);
    ChildLayerFirstClassLoader parentLoader =
        new ChildLayerFirstClassLoader(finder, Thread.currentThread().getContextClassLoader()) {

          @Override
          Stream<String> getNamesForModules(ModuleFinder finder) {
            return Arrays.asList("module.a", "module.b").stream();
          }

          @Override
          ChildFirstURLClassLoader getModuleLoader(String module) {
            return new ChildFirstURLClassLoader(new URL[] {}, this);
          }
        };

    parentLoader.setLayer(mock(ModuleLayer.class));
    Enumeration<URL> found = parentLoader.findResources("test-reference.conf");
    assertNotNull(found);
  }

  @Test
  public void testContextLoaderFence() {
    ClassLoader current = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = new URLClassLoader(new URL[] {}, getClass().getClassLoader());
    try (var ign = new ClassLoaders.ContextLoaderFence(loader)) {
      assertSame(loader, Thread.currentThread().getContextClassLoader());
    }
    assertSame(current, Thread.currentThread().getContextClassLoader());
  }
}
