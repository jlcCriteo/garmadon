package com.criteo.hadoop.garmadon.agent.modules;

import com.criteo.hadoop.garmadon.agent.utils.AgentAttachmentRule;
import com.criteo.hadoop.garmadon.agent.utils.ClassFileExtraction;
import com.criteo.hadoop.garmadon.schema.events.PathEvent;
import com.criteo.hadoop.garmadonnotexcluded.MapReduceInputFormatTestClasses;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ByteArrayClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

/**
 * IMPORTANT NOTE, we use a specific classloader to allow class redefinition between tests
 * We need to use reflection even for method calls because class casting cannot be cross classloader
 * and the test itself is not loaded with our custom classloader
 */
public class MapReduceInputFormatTracerTest {

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private Consumer<Object> eventHandler;
    private InputSplit inputSplit;
    private JobContext jobContext;
    private TaskAttemptContext taskAttemptContext;
    private Configuration conf;

    private ClassLoader classLoader;

    private String inputPath = "/some/inputpath,/some/other/path";
    private String deprecatedInputPath = "/some/inputpathDeprecated";

    @Before
    public void setUp() throws IOException {
        eventHandler = mock(Consumer.class);
        inputSplit = mock(InputSplit.class);
        jobContext = mock(JobContext.class);
        taskAttemptContext = mock(TaskAttemptContext.class);
        conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.inputdir", inputPath);

        when(jobContext.getConfiguration())
                .thenReturn(conf);
        when(taskAttemptContext.getConfiguration())
                .thenReturn(conf);

        when(jobContext.getJobName())
                .thenReturn("Application");
        when(jobContext.getUser())
                .thenReturn("user");
        when(taskAttemptContext.getJobName())
                .thenReturn("Application");
        when(taskAttemptContext.getUser())
                .thenReturn("user");

        JobID jobId = mock(JobID.class);
        when(jobId.toString())
                .thenReturn("app_1");
        when(jobContext.getJobID())
                .thenReturn(jobId);
        when(taskAttemptContext.getJobID())
                .thenReturn(jobId);

        classLoader = new ByteArrayClassLoader.ChildFirst(getClass().getClassLoader(),
                ClassFileExtraction.of(
                        MapReduceInputFormatTestClasses.OneLevelHierarchy.class
                ),
                ByteArrayClassLoader.PersistenceHandler.MANIFEST);
    }

    @After
    public void tearDown() {
        reset(eventHandler);
        reset(inputSplit);
        reset(jobContext);
        reset(taskAttemptContext);
    }

    /*
        We want to test if we intercept method impl in a one level class hierarchy
     */
    @Test
    @AgentAttachmentRule.Enforce
    public void InputFormatTracer_should_intercept_InputFormat_direct_implementor() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        assertThat(ByteBuddyAgent.install(), instanceOf(Instrumentation.class));

        //Install tracer
        ClassFileTransformer classFileTransformer = new MapReduceModule.InputFormatTracer(eventHandler).installOnByteBuddyAgent();
        try {
            //Call InputFormat
            Class<?> type = classLoader.loadClass(MapReduceInputFormatTestClasses.OneLevelHierarchy.class.getName());
            invokeRecordReader(type);
            invokeListInputSplits(type);

            //Verify mock interaction
            PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), inputPath, PathEvent.Type.INPUT);
            verify(eventHandler, times(2)).accept(pathEvent);
        } finally {
            ByteBuddyAgent.getInstrumentation().removeTransformer(classFileTransformer);
        }
    }

    /*
        We want to test that we get deprecated path if mapreduce one is not provided
     */
    @Test
    public void OutputFormatTracer_should_should_get_deprecated_value() throws Exception {
        // Configure deprecated output dir
        conf.unset("mapreduce.input.fileinputformat.inputdir");
        conf.set("mapred.input.dir", deprecatedInputPath);

        Constructor<MapReduceModule.InputFormatTracer> c = MapReduceModule.InputFormatTracer.class.getDeclaredConstructor(Consumer.class);
        c.setAccessible(true);
        MapReduceModule.InputFormatTracer u = c.newInstance(eventHandler);
        u.intercept(taskAttemptContext);
        PathEvent pathEvent = new PathEvent(System.currentTimeMillis(), deprecatedInputPath, PathEvent.Type.INPUT);
        verify(eventHandler).accept(pathEvent);
    }

    private Object invokeRecordReader(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("createRecordReader", InputSplit.class, TaskAttemptContext.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, inputSplit, taskAttemptContext);
    }

    private Object invokeListInputSplits(Class<?> type) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Method m = type.getMethod("getSplits", JobContext.class);
        Object inFormat = type.newInstance();
        return m.invoke(inFormat, jobContext);
    }


}
