package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class StorageItemFilterTest {

    @RunWith(MockitoJUnitRunner.class)
    public static class ValidationTest {

        @Mock
        private HttpServletRequest request;

        @Test(expected = NullPointerException.class)
        public void nullRequest() throws IOException {
            StorageItemFilter.getParameter(null, "file", null);
        }

        @Test(expected = IllegalArgumentException.class)
        public void nullParamName() throws IOException {
            StorageItemFilter.getParameter(request, null, null);
        }

        @Test(expected = IllegalArgumentException.class)
        public void blankParamName() throws IOException {
            StorageItemFilter.getParameter(request, "", null);
        }

        @Test
        public void blankParamValues() throws IOException {
            when(request.getParameterValues("paramName")).thenReturn(null);
            StorageItem storageItem = StorageItemFilter.getParameter(request, "paramName", null);
            assertTrue(storageItem == null);
            reset(request);
        }
    }

    @RunWith(MockitoJUnitRunner.class)
    public static class RequestTest {

        private StorageItemFilter filter;

        private HttpServletRequest request;

        @Mock
        private MultipartRequest mpRequest;

        @Mock
        private HttpServletResponse response;

        @Mock
        private FilterChain chain;

        @Mock
        private ServletContext servletContext;

        @Before
        public void before() {
            filter = new StorageItemFilter();
            request = mock(HttpServletRequest.class);
            doReturn("/_dari/upload").when(request).getServletPath();
            doReturn("file").when(request).getParameter("fileParameter");
            doReturn(Collections.emptyList()).when(request).getAttribute(AbstractFilter.class.getName() + ".dependencies." + StorageItemFilter.class.getName());
        }

        @After
        public void after() {
            filter = null;
            request = null;
        }

        private static final String STORAGE_KEY = "storage";
        private static final String PATH_KEY = "path";
        private static final String CONTENT_TYPE_KEY = "contentType";
        private static final String METADATA_KEY = "metadata";

        @Test
        public void doNothing() throws Exception {

            when(request.getServletPath()).thenReturn("/url");
            filter.doFilter(request, response, chain);
            verify(chain).doFilter(request, response);
        }

        @Test
        public void jsonRequest() throws Exception {

            String storageValue = "testStorage";
            String pathValue = "5d/ca/f0b343b34d2783e91bc74c42e422/test.jpg";
            String contentTypeValue = "image/jpeg";
            Map<String, Object> metadataValue = new HashMap<>();
            metadataValue.put("key", "value");

            setSettingsOverrides(storageValue);

            Map<String, Object> map = new HashMap<>();
            map.put(STORAGE_KEY, storageValue);
            map.put(PATH_KEY, pathValue);
            map.put(CONTENT_TYPE_KEY, contentTypeValue);
            map.put(METADATA_KEY, metadataValue);

            when(request.getParameterValues("file")).thenReturn(new String[] {ObjectUtils.toJson(map)});
            assertEquals(getJsonResponse(request, response, chain), map);
        }

        @Test
        public void fileRequest() throws Exception {

            String storageValue = "testStorage";
            String contentType = "image/png";
            String fileName = "image.png";

            setSettingsOverrides(storageValue);

            FileItem fileItem = spy(Utils.getFileItem(
                    "image/png",
                    fileName,
                    "com/psddev/dari/util/StorageItemFilter_Test/" + fileName));

            when(fileItem.isFormField()).thenReturn(false);
            when(mpRequest.getMethod()).thenReturn("POST");
            when(request.getAttribute(MultipartRequestFilter.class.getName() + ".instance")).thenReturn(mpRequest);
            when(mpRequest.getFileItems("file")).thenReturn(new FileItem[] { fileItem });

            Map<String, Object> jsonResponse = getJsonResponse(request, response, chain);

            assertEquals(jsonResponse.get(STORAGE_KEY), storageValue);
            assertEquals(jsonResponse.get(CONTENT_TYPE_KEY), contentType);
            assertTrue(jsonResponse.get(PATH_KEY).toString().endsWith(fileName));
        }

        private void setSettingsOverrides(String storage) {
            Settings.setOverride(StorageItem.DEFAULT_STORAGE_SETTING, storage);
            Settings.setOverride(StorageItem.SETTING_PREFIX + "/" + storage, ImmutableMap.of(
                    "class", TestStorageItem.class.getName(),
                    "rootPath", "/webapps/media-files",
                    "baseUrl", "http://localhost:8080/media-files"
            ));
        }

        private Map<String, Object> getJsonResponse(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws Exception {
            StringWriter writer = new StringWriter();
            when(response.getWriter()).thenReturn(new PrintWriter(writer));
            filter.doFilter(request, response, chain);

            return ObjectUtils.to(
                    new TypeReference<Map<String, Object>>() {
                    },
                    ObjectUtils.fromJson(writer.toString()));
        }

        public static final class TestStorageItem extends LocalStorageItem {

            @Override
            protected void saveData(InputStream data) throws IOException {
            }
        }
    }

    @Ignore
    public static class Utils {

        @Ignore
        public static FileItem getFileItem(String contentType, String fileName, String filePath) throws IOException {
            FileItem fileItem = new DiskFileItemFactory().createItem(
                    "file",
                    contentType,
                    true,
                    fileName);

            if (!StringUtils.isBlank(filePath)) {
                OutputStream os = fileItem.getOutputStream();
                os.write(IoUtils.toByteArray(StorageItemFilterTest.class.getClassLoader().getResourceAsStream(filePath)));
                os.close();
            }

            return fileItem;
        }
    }
}
