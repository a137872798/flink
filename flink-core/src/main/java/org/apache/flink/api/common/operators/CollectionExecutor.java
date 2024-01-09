/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.BulkIterationBase.PartialSolutionPlaceHolder;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.api.common.operators.base.DeltaIterationBase.SolutionSetPlaceHolder;
import org.apache.flink.api.common.operators.base.DeltaIterationBase.WorksetPlaceHolder;
import org.apache.flink.api.common.operators.util.TypeComparable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.types.Value;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Execution utility for serial, local, collection-based executions of Flink programs.
 * 执行flink程序的工具
 * */
@Internal
public class CollectionExecutor {

    /**
     * 存储中间结果集
     * 在整个执行过程中 可能会出现多个operator 每个operator都会产生一组中间结果集  通过该对象维护
     */
    private final Map<Operator<?>, List<?>> intermediateResults;

    /**
     * 每个key 对应一个累加器 存储累加结果
     */
    private final Map<String, Accumulator<?, ?>> accumulators;

    /**
     * 缓存文件
     */
    private final Map<String, Future<Path>> cachedFiles;

    /**
     * 之前的聚合结果
     */
    private final Map<String, Value> previousAggregates;

    /**
     * 每个key 对应一个聚合器
     */
    private final Map<String, Aggregator<?>> aggregators;

    /**
     * 加载用户定义对象的 类加载器
     */
    private final ClassLoader userCodeClassLoader;

    /**
     * 执行配置参数
     */
    private final ExecutionConfig executionConfig;

    private int iterationSuperstep;

    // --------------------------------------------------------------------------------------------

    public CollectionExecutor(ExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;

        this.intermediateResults = new HashMap<Operator<?>, List<?>>();
        this.accumulators = new HashMap<String, Accumulator<?, ?>>();
        this.previousAggregates = new HashMap<String, Value>();
        this.aggregators = new HashMap<String, Aggregator<?>>();
        this.cachedFiles = new HashMap<String, Future<Path>>();
        this.userCodeClassLoader = Thread.currentThread().getContextClassLoader();
    }

    // --------------------------------------------------------------------------------------------
    //  General execution methods
    // --------------------------------------------------------------------------------------------

    /**
     *
     * @param program    plan 简单看过去就是几个 GenericDataSinkBase   每个的作用就是将一组输入排序后借助 outputFormat进行输出
     * @return
     * @throws Exception
     */
    public JobExecutionResult execute(Plan program) throws Exception {
        long startTime = System.currentTimeMillis();

        // 这是plan 关联的job  new JobID() 会产生一个随机数
        JobID jobID = program.getJobId() == null ? new JobID() : program.getJobId();

        // 根据分布式缓存对象记录的路径 加载文件
        initCache(program.getCachedFiles());

        // 获取plan相关的一组数据下沉对象
        Collection<? extends GenericDataSinkBase<?>> sinks = program.getDataSinks();

        // 挨个执行sink对象
        for (Operator<?> sink : sinks) {
            execute(sink, jobID);
        }

        long endTime = System.currentTimeMillis();

        // 认为在execute时 accumulators内应当已经存储了结果  现在将他们取出来
        Map<String, OptionalFailure<Object>> accumulatorResults =
                AccumulatorHelper.toResultMap(accumulators);

        // 包装结果
        return new JobExecutionResult(null, endTime - startTime, accumulatorResults);
    }

    /**
     * 初始化缓存对象 异步拉取
     * @param files
     */
    private void initCache(Set<Map.Entry<String, DistributedCache.DistributedCacheEntry>> files) {
        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> file : files) {
            Future<Path> doNothing = new CompletedFuture(new Path(file.getValue().filePath));
            cachedFiles.put(file.getKey(), doNothing);
        }
    }

    /**
     * 执行某个operator
     * @param operator
     * @param jobID
     * @return
     * @throws Exception
     */
    private List<?> execute(Operator<?> operator, JobID jobID) throws Exception {
        return execute(operator, 0, jobID);
    }

    /**
     *
     * @param operator   本次要执行的操作
     * @param superStep  代表从第几步开始  默认为0
     * @param jobID
     * @return
     * @throws Exception
     */
    private List<?> execute(Operator<?> operator, int superStep, JobID jobID) throws Exception {

        // 既然要执行这个operator  先尝试获取中间结果集
        List<?> result = this.intermediateResults.get(operator);

        // if it has already been computed, use the cached variant
        // 表示这个operator之前已经计算过 不需要重复计算 直接使用缓存的结果
        if (result != null) {
            return result;
        }

        // 这个也是要进行多次迭代的
        if (operator instanceof BulkIterationBase) {
            result = executeBulkIteration((BulkIterationBase<?>) operator, jobID);

            // 进行n次迭代 每次迭代计算一个增量结果
        } else if (operator instanceof DeltaIterationBase) {
            result = executeDeltaIteration((DeltaIterationBase<?, ?>) operator, jobID);

            // 代表是一个 一元输入操作
        } else if (operator instanceof SingleInputOperator) {
            result =
                    executeUnaryOperator((SingleInputOperator<?, ?, ?>) operator, superStep, jobID);

            // 代表是一个 二元输入操作
        } else if (operator instanceof DualInputOperator) {
            result =
                    executeBinaryOperator(
                            (DualInputOperator<?, ?, ?, ?>) operator, superStep, jobID);

            // 代表执行的是一个从source拉取数据的操作 从inputFormat读取
        } else if (operator instanceof GenericDataSourceBase) {
            result = executeDataSource((GenericDataSourceBase<?, ?>) operator, superStep, jobID);

            // 代表执行的是一个sink操作 用于将数据通过 outputFormat写出
        } else if (operator instanceof GenericDataSinkBase) {
            executeDataSink((GenericDataSinkBase<?>) operator, superStep, jobID);
            result = Collections.emptyList();
        } else {
            throw new RuntimeException("Cannot execute operator " + operator.getClass().getName());
        }

        // 得到结果后 存入中间结果集 避免重复计算
        this.intermediateResults.put(operator, result);

        return result;
    }

    // --------------------------------------------------------------------------------------------
    //  Operator class specific execution methods
    // --------------------------------------------------------------------------------------------

    /**
     * 处理数据汇对象
     * @param sink
     * @param superStep
     * @param jobID
     * @param <IN>
     * @throws Exception
     */
    private <IN> void executeDataSink(GenericDataSinkBase<?> sink, int superStep, JobID jobID)
            throws Exception {
        Operator<?> inputOp = sink.getInput();
        if (inputOp == null) {
            throw new InvalidProgramException("The data sink " + sink.getName() + " has no input.");
        }

        // 可以看到会一层层往上传递  在获得要往下写的数据前 先要获得原始数据
        @SuppressWarnings("unchecked")
        List<IN> input = (List<IN>) execute(inputOp, jobID);

        @SuppressWarnings("unchecked")
        GenericDataSinkBase<IN> typedSink = (GenericDataSinkBase<IN>) sink;

        // build the runtime context and compute broadcast variables, if necessary
        // 生成一个task的描述信息
        TaskInfo taskInfo = new TaskInfo(typedSink.getName(), 1, 0, 1, 0);
        RuntimeUDFContext ctx;

        if (RichOutputFormat.class.isAssignableFrom(
                typedSink.getUserCodeWrapper().getUserCodeClass())) {
            // 生成上下文对象
            ctx = createContext(superStep, taskInfo, jobID);
        } else {
            ctx = null;
        }

        // 在outputFormat中 可能会需要context信息 所以这里要传入 在executeOnCollections之后 数据就被写入到sink中了
        typedSink.executeOnCollections(input, ctx, executionConfig);
    }

    /**
     * 根据相关信息 生成上下文对象
     * @param superStep
     * @param taskInfo
     * @param jobID
     * @return
     */
    private RuntimeUDFContext createContext(int superStep, TaskInfo taskInfo, JobID jobID) {
        OperatorMetricGroup metrics = UnregisteredMetricsGroup.createOperatorMetricGroup();
        return superStep == 0

                // 第0步创建该对象
                ? new RuntimeUDFContext(
                        taskInfo,
                        userCodeClassLoader,
                        executionConfig,
                        cachedFiles,
                        accumulators,
                        metrics,
                        jobID)
                // 如果是多步 创建该对象   也是生成上下文对象 区别是多几个字段
                : new IterationRuntimeUDFContext(
                        taskInfo,
                        userCodeClassLoader,
                        executionConfig,
                        cachedFiles,
                        accumulators,
                        metrics,
                        jobID);
    }

    /**
     * 处理输入源
     * @param source
     * @param superStep
     * @param jobID
     * @param <OUT>
     * @return
     * @throws Exception
     */
    private <OUT> List<OUT> executeDataSource(
            GenericDataSourceBase<?, ?> source, int superStep, JobID jobID) throws Exception {
        @SuppressWarnings("unchecked")
        GenericDataSourceBase<OUT, ?> typedSource = (GenericDataSourceBase<OUT, ?>) source;
        // build the runtime context and compute broadcast variables, if necessary
        TaskInfo taskInfo = new TaskInfo(typedSource.getName(), 1, 0, 1, 0);

        RuntimeUDFContext ctx;

        if (RichInputFormat.class.isAssignableFrom(
                typedSource.getUserCodeWrapper().getUserCodeClass())) {
            ctx = createContext(superStep, taskInfo, jobID);
        } else {
            ctx = null;
        }

        // 将会采集数据 并返回   一次性加载所有数据吗？ 这样对内存开销会不会太大?
        return typedSource.executeOnCollections(ctx, executionConfig);
    }

    /**
     * 处理一元输入的操作
     * @param operator
     * @param superStep
     * @param jobID
     * @param <IN>
     * @param <OUT>
     * @return
     * @throws Exception
     */
    private <IN, OUT> List<OUT> executeUnaryOperator(
            SingleInputOperator<?, ?, ?> operator, int superStep, JobID jobID) throws Exception {
        Operator<?> inputOp = operator.getInput();
        if (inputOp == null) {
            throw new InvalidProgramException(
                    "The unary operation " + operator.getName() + " has no input.");
        }

        // 计算产生输入数据
        @SuppressWarnings("unchecked")
        List<IN> inputData = (List<IN>) execute(inputOp, superStep, jobID);

        @SuppressWarnings("unchecked")
        SingleInputOperator<IN, OUT, ?> typedOp = (SingleInputOperator<IN, OUT, ?>) operator;

        // build the runtime context and compute broadcast variables, if necessary
        TaskInfo taskInfo = new TaskInfo(typedOp.getName(), 1, 0, 1, 0);
        RuntimeUDFContext ctx;

        if (RichFunction.class.isAssignableFrom(typedOp.getUserCodeWrapper().getUserCodeClass())) {
            ctx = createContext(superStep, taskInfo, jobID);

            // 计算并存储广播数据到上下文中
            for (Map.Entry<String, Operator<?>> bcInputs :
                    operator.getBroadcastInputs().entrySet()) {
                List<?> bcData = execute(bcInputs.getValue(), jobID);
                ctx.setBroadcastVariable(bcInputs.getKey(), bcData);
            }
        } else {
            ctx = null;
        }

        return typedOp.executeOnCollections(inputData, ctx, executionConfig);
    }

    /**
     * 执行一个二元输入操作
     * @param operator
     * @param superStep
     * @param jobID
     * @param <IN1>
     * @param <IN2>
     * @param <OUT>
     * @return
     * @throws Exception
     */
    private <IN1, IN2, OUT> List<OUT> executeBinaryOperator(
            DualInputOperator<?, ?, ?, ?> operator, int superStep, JobID jobID) throws Exception {
        Operator<?> inputOp1 = operator.getFirstInput();
        Operator<?> inputOp2 = operator.getSecondInput();

        if (inputOp1 == null) {
            throw new InvalidProgramException(
                    "The binary operation " + operator.getName() + " has no first input.");
        }
        if (inputOp2 == null) {
            throw new InvalidProgramException(
                    "The binary operation " + operator.getName() + " has no second input.");
        }

        // compute inputs   分别处理2个输入 得到中间结果集
        @SuppressWarnings("unchecked")
        List<IN1> inputData1 = (List<IN1>) execute(inputOp1, superStep, jobID);
        @SuppressWarnings("unchecked")
        List<IN2> inputData2 = (List<IN2>) execute(inputOp2, superStep, jobID);

        @SuppressWarnings("unchecked")
        DualInputOperator<IN1, IN2, OUT, ?> typedOp =
                (DualInputOperator<IN1, IN2, OUT, ?>) operator;

        // build the runtime context and compute broadcast variables, if necessary
        TaskInfo taskInfo = new TaskInfo(typedOp.getName(), 1, 0, 1, 0);
        RuntimeUDFContext ctx;

        if (RichFunction.class.isAssignableFrom(typedOp.getUserCodeWrapper().getUserCodeClass())) {
            ctx = createContext(superStep, taskInfo, jobID);

            for (Map.Entry<String, Operator<?>> bcInputs :
                    operator.getBroadcastInputs().entrySet()) {
                // 如果有广播输入  全部执行 这些将作为参数 设置到上下文中
                List<?> bcData = execute(bcInputs.getValue(), jobID);
                ctx.setBroadcastVariable(bcInputs.getKey(), bcData);
            }
        } else {
            ctx = null;
        }

        // 将输入和上下文信息传入 计算产生结果
        return typedOp.executeOnCollections(inputData1, inputData2, ctx, executionConfig);
    }

    /**
     * 处理 bulkIteration 对象
     * @param iteration
     * @param jobID
     * @param <T>
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <T> List<T> executeBulkIteration(BulkIterationBase<?> iteration, JobID jobID)
            throws Exception {
        Operator<?> inputOp = iteration.getInput();
        if (inputOp == null) {
            throw new InvalidProgramException(
                    "The iteration "
                            + iteration.getName()
                            + " has no input (initial partial solution).");
        }
        if (iteration.getNextPartialSolution() == null) {
            throw new InvalidProgramException(
                    "The iteration "
                            + iteration.getName()
                            + " has no next partial solution defined (is not closed).");
        }

        // 先计算输入数据
        List<T> inputData = (List<T>) execute(inputOp, jobID);

        // get the operators that are iterative
        Set<Operator<?>> dynamics = new LinkedHashSet<Operator<?>>();

        // 同样需要借助该对象记录迭代过程中 经过的operator 在每次遍历前 删除之前的数据
        DynamicPathCollector dynCollector = new DynamicPathCollector(dynamics);
        iteration.getNextPartialSolution().accept(dynCollector);
        if (iteration.getTerminationCriterion() != null) {
            iteration.getTerminationCriterion().accept(dynCollector);
        }

        // register the aggregators
        // 存储当前聚合对象
        for (AggregatorWithName<?> a : iteration.getAggregators().getAllRegisteredAggregators()) {
            aggregators.put(a.getName(), a.getAggregator());
        }

        // 该对象用于判断能否提前结束迭代
        String convCriterionAggName =
                iteration.getAggregators().getConvergenceCriterionAggregatorName();
        ConvergenceCriterion<Value> convCriterion =
                (ConvergenceCriterion<Value>) iteration.getAggregators().getConvergenceCriterion();

        List<T> currentResult = inputData;

        final int maxIterations = iteration.getMaximumNumberOfIterations();

        // 进入循环
        for (int superstep = 1; superstep <= maxIterations; superstep++) {

            // set the input to the current partial solution
            this.intermediateResults.put(iteration.getPartialSolution(), currentResult);

            // set the superstep number    更新当前步骤
            iterationSuperstep = superstep;

            // grab the current iteration result
            // 每次要重新计算的就是 NextPartialSolution
            currentResult = (List<T>) execute(iteration.getNextPartialSolution(), superstep, jobID);

            // evaluate the termination criterion
            if (iteration.getTerminationCriterion() != null) {
                execute(iteration.getTerminationCriterion(), superstep, jobID);
            }

            // evaluate the aggregator convergence criterion
            // 判断能否提前结束迭代
            if (convCriterion != null && convCriterionAggName != null) {
                Value v = aggregators.get(convCriterionAggName).getAggregate();
                if (convCriterion.isConverged(superstep, v)) {
                    break;
                }
            }

            // clear the dynamic results
            // dynamics 中会记录本次链路相关的所有中间结果集  现在将这些结果集移除 以便下次重新计算
            for (Operator<?> o : dynamics) {
                intermediateResults.remove(o);
            }

            // set the previous iteration's aggregates and reset the aggregators
            // 将本次聚合结果作为  previous聚合结果
            for (Map.Entry<String, Aggregator<?>> e : aggregators.entrySet()) {
                previousAggregates.put(e.getKey(), e.getValue().getAggregate());
                e.getValue().reset();
            }
        }

        // 在处理完后 清理中间的聚合结果
        previousAggregates.clear();
        aggregators.clear();

        return currentResult;
    }

    /**
     * 处理一个增量迭代对象
     * @param iteration
     * @param jobID
     * @param <T>
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <T> List<T> executeDeltaIteration(DeltaIterationBase<?, ?> iteration, JobID jobID)
            throws Exception {
        Operator<?> solutionInput = iteration.getInitialSolutionSet();
        Operator<?> worksetInput = iteration.getInitialWorkset();

        // 这些参数必须提前准备好
        if (solutionInput == null) {
            throw new InvalidProgramException(
                    "The delta iteration " + iteration.getName() + " has no initial solution set.");
        }
        if (worksetInput == null) {
            throw new InvalidProgramException(
                    "The delta iteration " + iteration.getName() + " has no initial workset.");
        }
        if (iteration.getSolutionSetDelta() == null) {
            throw new InvalidProgramException(
                    "The iteration "
                            + iteration.getName()
                            + " has no solution set delta defined (is not closed).");
        }
        if (iteration.getNextWorkset() == null) {
            throw new InvalidProgramException(
                    "The iteration "
                            + iteration.getName()
                            + " has no workset defined (is not closed).");
        }

        // 借助这2个输入 产生数据
        List<T> solutionInputData = (List<T>) execute(solutionInput, jobID);
        List<T> worksetInputData = (List<T>) execute(worksetInput, jobID);

        // get the operators that are iterative
        Set<Operator<?>> dynamics = new LinkedHashSet<Operator<?>>();

        // 该对象会记录operator
        DynamicPathCollector dynCollector = new DynamicPathCollector(dynamics);
        // 使用collector 记录2个数据集的数据
        iteration.getSolutionSetDelta().accept(dynCollector);
        iteration.getNextWorkset().accept(dynCollector);

        // 获取操作的描述信息
        BinaryOperatorInformation<?, ?, ?> operatorInfo = iteration.getOperatorInfo();
        TypeInformation<?> solutionType = operatorInfo.getFirstInputType();

        // 创建比较器
        int[] keyColumns = iteration.getSolutionSetKeyFields();
        boolean[] inputOrderings = new boolean[keyColumns.length];
        TypeComparator<T> inputComparator =
                ((CompositeType<T>) solutionType)
                        .createComparator(keyColumns, inputOrderings, 0, executionConfig);

        Map<TypeComparable<T>, T> solutionMap =
                new HashMap<TypeComparable<T>, T>(solutionInputData.size());
        // fill the solution from the initial input
        // 把该input的每个数据 包装成可比较对象后返回
        for (T delta : solutionInputData) {
            TypeComparable<T> wrapper = new TypeComparable<T>(delta, inputComparator);
            solutionMap.put(wrapper, delta);
        }

        List<?> currentWorkset = worksetInputData;

        // register the aggregators
        // 存储所有使用的聚合器
        for (AggregatorWithName<?> a : iteration.getAggregators().getAllRegisteredAggregators()) {
            aggregators.put(a.getName(), a.getAggregator());
        }

        // 获取一个名字
        String convCriterionAggName =
                iteration.getAggregators().getConvergenceCriterionAggregatorName();

        // 该对象可以判断迭代是否可以收敛  (估计是提前结束的意思?)
        ConvergenceCriterion<Value> convCriterion =
                (ConvergenceCriterion<Value>) iteration.getAggregators().getConvergenceCriterion();

        // 最大迭代次数
        final int maxIterations = iteration.getMaximumNumberOfIterations();

        // 处理该对象就是开始迭代了
        for (int superstep = 1; superstep <= maxIterations; superstep++) {

            List<T> currentSolution = new ArrayList<T>(solutionMap.size());
            // 该容器中存储了所有输入
            currentSolution.addAll(solutionMap.values());

            // set the input to the current partial solution
            // 将当前输入 作为中间结果集存储到容器中
            this.intermediateResults.put(iteration.getSolutionSet(), currentSolution);
            this.intermediateResults.put(iteration.getWorkset(), currentWorkset);

            // set the superstep number
            // 更新当前子任务的迭代次数
            iterationSuperstep = superstep;

            // grab the current iteration result
            // 获得增量数据
            List<T> solutionSetDelta =
                    (List<T>) execute(iteration.getSolutionSetDelta(), superstep, jobID);
            // 将计算出来的增量数据也作为中间结果集
            this.intermediateResults.put(iteration.getSolutionSetDelta(), solutionSetDelta);

            // update the solution
            // 更新结果集
            for (T delta : solutionSetDelta) {
                TypeComparable<T> wrapper = new TypeComparable<T>(delta, inputComparator);
                solutionMap.put(wrapper, delta);
            }

            // 获取 nextWorkset的数据
            currentWorkset = execute(iteration.getNextWorkset(), superstep, jobID);

            if (currentWorkset.isEmpty()) {
                break;
            }

            // evaluate the aggregator convergence criterion
            if (convCriterion != null && convCriterionAggName != null) {
                Value v = aggregators.get(convCriterionAggName).getAggregate();
                // 如果收敛了  提前退出
                if (convCriterion.isConverged(superstep, v)) {
                    break;
                }
            }

            // clear the dynamic results
            for (Operator<?> o : dynamics) {
                intermediateResults.remove(o);
            }

            // set the previous iteration's aggregates and reset the aggregators
            for (Map.Entry<String, Aggregator<?>> e : aggregators.entrySet()) {
                previousAggregates.put(e.getKey(), e.getValue().getAggregate());
                e.getValue().reset();
            }
        }

        previousAggregates.clear();
        aggregators.clear();

        List<T> currentSolution = new ArrayList<T>(solutionMap.size());
        currentSolution.addAll(solutionMap.values());
        return currentSolution;
    }

    // --------------------------------------------------------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * 作为一个访问者对象
     */
    private static final class DynamicPathCollector implements Visitor<Operator<?>> {

        /**
         * 主要是避免重复访问
         */
        private final Set<Operator<?>> visited = new HashSet<Operator<?>>();

        /**
         * 动态路径操作对象
         */
        private final Set<Operator<?>> dynamicPathOperations;

        public DynamicPathCollector(Set<Operator<?>> dynamicPathOperations) {
            this.dynamicPathOperations = dynamicPathOperations;
        }

        /**
         * @param op
         * @return
         */
        @Override
        public boolean preVisit(Operator<?> op) {
            return visited.add(op);
        }

        /**
         * 后置处理
         * @param op
         */
        @Override
        public void postVisit(Operator<?> op) {

            // 传入的是一个单输入对象
            if (op instanceof SingleInputOperator) {
                SingleInputOperator<?, ?, ?> siop = (SingleInputOperator<?, ?, ?>) op;

                // 如果包含该操作的输入 那么将本操作也加入到 operations中
                if (dynamicPathOperations.contains(siop.getInput())) {
                    dynamicPathOperations.add(op);
                } else {
                    // 如果前置参数包含在内 就将本operator加入到容器中
                    for (Operator<?> o : siop.getBroadcastInputs().values()) {
                        if (dynamicPathOperations.contains(o)) {
                            dynamicPathOperations.add(op);
                            break;
                        }
                    }
                }
                // 传入的是一个双输入对象
            } else if (op instanceof DualInputOperator) {
                DualInputOperator<?, ?, ?, ?> siop = (DualInputOperator<?, ?, ?, ?>) op;

                // 只要有任何一个输入存在 就将本对象加入到列表中
                if (dynamicPathOperations.contains(siop.getFirstInput())) {
                    dynamicPathOperations.add(op);
                } else if (dynamicPathOperations.contains(siop.getSecondInput())) {
                    dynamicPathOperations.add(op);
                } else {
                    for (Operator<?> o : siop.getBroadcastInputs().values()) {
                        if (dynamicPathOperations.contains(o)) {
                            dynamicPathOperations.add(op);
                            break;
                        }
                    }
                }

                // 如果是特殊类型的operator 直接加入容器
            } else if (op.getClass() == PartialSolutionPlaceHolder.class
                    || op.getClass() == WorksetPlaceHolder.class
                    || op.getClass() == SolutionSetPlaceHolder.class) {
                dynamicPathOperations.add(op);

                // 如果是数据源 则忽略
            } else if (op instanceof GenericDataSourceBase) {
                // skip
            } else {
                throw new RuntimeException(
                        "Cannot handle operator type " + op.getClass().getName());
            }
        }
    }

    private class IterationRuntimeUDFContext extends RuntimeUDFContext
            implements IterationRuntimeContext {

        public IterationRuntimeUDFContext(
                TaskInfo taskInfo,
                ClassLoader classloader,
                ExecutionConfig executionConfig,
                Map<String, Future<Path>> cpTasks,
                Map<String, Accumulator<?, ?>> accumulators,
                OperatorMetricGroup metrics,
                JobID jobID) {
            super(taskInfo, classloader, executionConfig, cpTasks, accumulators, metrics, jobID);
        }

        /**
         * 获取子任务当前的迭代次数
         * @return
         */
        @Override
        public int getSuperstepNumber() {
            return iterationSuperstep;
        }

        // 可以提前获得聚合结果

        @SuppressWarnings("unchecked")
        @Override
        public <T extends Aggregator<?>> T getIterationAggregator(String name) {
            return (T) aggregators.get(name);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends Value> T getPreviousIterationAggregate(String name) {
            return (T) previousAggregates.get(name);
        }
    }

    /**
     * 包含一个异步处理结果
     */
    private static final class CompletedFuture implements Future<Path> {

        private final Path result;

        public CompletedFuture(Path entry) {
            try {
                LocalFileSystem fs =
                        (LocalFileSystem) FileSystem.getUnguardedFileSystem(entry.toUri());

                // 解析entry 得到一个结果
                result =
                        entry.isAbsolute()
                                ? new Path(entry.toUri().getPath())
                                : new Path(fs.getWorkingDirectory(), entry);
            } catch (Exception e) {
                throw new RuntimeException(
                        "DistributedCache supports only local files for Collection Environments");
            }
        }

        // 可以看到该任务是无法取消的
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Path get() throws InterruptedException, ExecutionException {
            return result;
        }

        @Override
        public Path get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }
    }
}
