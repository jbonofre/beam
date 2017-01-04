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

package org.apache.beam.runners.spark;

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.aggregators.SparkAggregators;
import org.apache.beam.runners.spark.aggregators.metrics.AggregatorMetricSource;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkEnv$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The SparkRunner translate operations defined on a pipeline to a representation
 * executable by Spark, and then submitting the job to Spark to be executed. If we wanted to run
 * a Beam pipeline with the default options of a single threaded spark instance in local mode,
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineResult result = (SparkPipelineResult) p.run();
 * }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run();
 * }
 */
public final class SparkRunner extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);
  /**
   * Options used in this pipeline runner.
   */
  private final SparkPipelineOptions mOptions;

  private final Map<PCollection, Long> cacheCandidates = new HashMap();

  /**
   * Creates and returns a new SparkRunner with default options. In particular, against a
   * spark instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkRunner create() {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The SparkPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner create(SparkPipelineOptions options) {
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner fromOptions(PipelineOptions options) {
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
    return new SparkRunner(sparkOptions);
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkRunner(SparkPipelineOptions options) {
    mOptions = options;
  }

  private void registerMetrics(final SparkPipelineOptions opts, final JavaSparkContext jsc) {
    final Accumulator<NamedAggregators> accum = SparkAggregators.getNamedAggregators(jsc);
    final NamedAggregators initialValue = accum.value();

    if (opts.getEnableSparkMetricSinks()) {
      final MetricsSystem metricsSystem = SparkEnv$.MODULE$.get().metricsSystem();
      final AggregatorMetricSource aggregatorMetricSource =
          new AggregatorMetricSource(opts.getAppName(), initialValue);
      // re-register the metrics in case of context re-use
      metricsSystem.removeSource(aggregatorMetricSource);
      metricsSystem.registerSource(aggregatorMetricSource);
    }
  }

  @Override
  public SparkPipelineResult run(final Pipeline pipeline) {
    LOG.info("Executing pipeline using the SparkRunner.");

    final SparkPipelineResult result;
    final Future<?> startPipeline;
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    detectTranslationMode(pipeline);

    if (mOptions.isStreaming()) {
      final SparkRunnerStreamingContextFactory contextFactory =
          new SparkRunnerStreamingContextFactory(pipeline, mOptions, cacheCandidates);
      final JavaStreamingContext jssc =
          JavaStreamingContext.getOrCreate(mOptions.getCheckpointDir(), contextFactory);

      startPipeline = executorService.submit(new Runnable() {

        @Override
        public void run() {
          registerMetrics(mOptions, jssc.sparkContext());
          LOG.info("Starting streaming pipeline execution.");
          jssc.start();
        }
      });

      result = new SparkPipelineResult.StreamingMode(startPipeline, jssc);
    } else {
      final JavaSparkContext jsc = SparkContextFactory.getSparkContext(mOptions);
      final EvaluationContext evaluationContext = new EvaluationContext(jsc, pipeline);

      startPipeline = executorService.submit(new Runnable() {

        @Override
        public void run() {
          registerMetrics(mOptions, jsc);
          pipeline.traverseTopologically(new Evaluator(new TransformTranslator.Translator(),
                                                       evaluationContext, cacheCandidates));
          evaluationContext.computeOutputs();
          LOG.info("Batch pipeline execution complete.");
        }
      });

      result = new SparkPipelineResult.BatchMode(startPipeline, jsc);
    }

    return result;
  }

  /**
   * Detect the translation mode for the pipeline and change options in case streaming
   * translation is needed.
   * @param pipeline
   */
  private void detectTranslationMode(Pipeline pipeline) {
    DAGPreVisit detector = new DAGPreVisit(cacheCandidates);
    pipeline.traverseTopologically(detector);
    if (detector.getTranslationMode().equals(TranslationMode.STREAMING)) {
      // set streaming mode if it's a streaming pipeline
      this.mOptions.setStreaming(true);
    }
  }

  /**
   * The translation mode of the Beam Pipeline.
   */
  enum TranslationMode {
    /** Uses the batch mode. */
    BATCH,
    /** Uses the streaming mode. */
    STREAMING
  }

  /**
   * Traverses the Pipeline to determine the {@link TranslationMode} and add the candidates for
   * caching for this pipeline. It's the preparation step of the runner.
   */
  static class DAGPreVisit extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(DAGPreVisit.class);

    private TranslationMode translationMode;
    private Map<PCollection, Long> cacheCandidates;

    DAGPreVisit(TranslationMode defaultMode, Map<PCollection, Long> cacheCandidates) {
      this.translationMode = defaultMode;
      this.cacheCandidates = cacheCandidates;
    }

    DAGPreVisit(Map<PCollection, Long> cacheCandidates) {
      this(TranslationMode.BATCH, cacheCandidates);
    }

    TranslationMode getTranslationMode() {
      return translationMode;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      // we populate the cache candidates by updating the map with inputs of each node.
      // The purpose is to detect the PCollection more than one time, and so enable cache on the
      // underlying RDDs or DStream.
      // A PCollection (RDD/DStream) needs to be cached only if it's used as an input to more
      // than one transformation so it won't be evaluated again all the way throughout its lineage.
      // update cache candidates with node inputs
      for (TaggedPValue input : node.getInputs()) {
        PValue value = input.getValue();
        if (value instanceof PCollection) {
          long count = 1L;
          if (cacheCandidates.get(value) != null) {
            count = cacheCandidates.get(value) + 1;
          }
          if (value.getName().equals("Write/WriteBundles.out")) {
            // hack forcing caching for WriteBundles
            count = 2L;
          }
          cacheCandidates.put(((PCollection) value), count);
        }
      }
      // determine the translation mode of the pipeline. If the node transform (the source
      // actually) is a Read.Unbounded, then, we are in streaming mode, else, we are in batch mode.
      if (translationMode.equals(TranslationMode.BATCH)) {
        Class<? extends PTransform> transformClass = node.getTransform().getClass();
        if (transformClass == Read.Unbounded.class) {
          LOG.info("Found {}. Switching to streaming execution.", transformClass);
          translationMode = TranslationMode.STREAMING;
        }
      }
    }
  }

  /**
   * Evaluator on the pipeline.
   */
  public static class Evaluator extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

    private final EvaluationContext ctxt;
    private final SparkPipelineTranslator translator;
    private final Map<PCollection, Long> cacheCandidates;

    public Evaluator(SparkPipelineTranslator translator, EvaluationContext ctxt,
                     Map<PCollection, Long> cacheCandidates) {
      this.translator = translator;
      this.ctxt = ctxt;
      this.cacheCandidates = cacheCandidates;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      if (node.getTransform() != null) {
        @SuppressWarnings("unchecked")
        Class<PTransform<?, ?>> transformClass =
            (Class<PTransform<?, ?>>) node.getTransform().getClass();
        if (translator.hasTranslation(transformClass) && !shouldDefer(node)) {
          LOG.info("Entering directly-translatable composite transform: '{}'", node.getFullName());
          LOG.debug("Composite transform class: '{}'", transformClass);
          doVisitTransform(node);
          return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    private boolean shouldDefer(TransformHierarchy.Node node) {
      // if the input is not a PCollection, or it is but with non merging windows, don't defer.
      if (node.getInputs().size() != 1) {
        return false;
      }
      PValue input = Iterables.getOnlyElement(node.getInputs()).getValue();
      if (!(input instanceof PCollection)
          || ((PCollection) input).getWindowingStrategy().getWindowFn().isNonMerging()) {
        return false;
      }
      // so far we know that the input is a PCollection with merging windows.
      // check for sideInput in case of a Combine transform.
      PTransform<?, ?> transform = node.getTransform();
      boolean hasSideInput = false;
      if (transform instanceof Combine.PerKey) {
        List<PCollectionView<?>> sideInputs = ((Combine.PerKey<?, ?, ?>) transform).getSideInputs();
        hasSideInput = sideInputs != null && !sideInputs.isEmpty();
      } else if (transform instanceof Combine.Globally) {
        List<PCollectionView<?>> sideInputs = ((Combine.Globally<?, ?>) transform).getSideInputs();
        hasSideInput = sideInputs != null && !sideInputs.isEmpty();
      }
      // defer if sideInputs are defined.
      if (hasSideInput) {
        LOG.info("Deferring combine transformation {} for job {}", transform,
            ctxt.getPipeline().getOptions().getJobName());
        return true;
      }
      // default.
      return false;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      doVisitTransform(node);
    }

    <TransformT extends PTransform<? super PInput, POutput>> void
        doVisitTransform(TransformHierarchy.Node node) {
      @SuppressWarnings("unchecked")
      TransformT transform = (TransformT) node.getTransform();
      @SuppressWarnings("unchecked")
      Class<TransformT> transformClass = (Class<TransformT>) (Class<?>) transform.getClass();
      @SuppressWarnings("unchecked") TransformEvaluator<TransformT> evaluator =
          translate(node, transform, transformClass);
      LOG.info("Evaluating {}", transform);
      AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform();
      ctxt.setCurrentTransform(appliedTransform);
      // by default we don't cache
      boolean cacheHint = false;
      // if the output of the node (aka transform) is already known in the cache
      // candidates map (initialized with inputs), and it appears more than one time, then we
      // enable caching
      // considering node output for caching
      for (TaggedPValue output : node.getOutputs()) {
        PValue value = output.getValue();
        if ((value instanceof PCollection)
            && cacheCandidates.containsKey(value)
            && cacheCandidates.get(value) > 1) {
          cacheHint = true;
          break;
        }
      }
      evaluator.evaluate(transform, ctxt, cacheHint);
      ctxt.setCurrentTransform(null);
    }

    /**
     * Determine if this Node belongs to a Bounded branch of the pipeline, or Unbounded, and
     * translate with the proper translator.
     */
    private <TransformT extends PTransform<? super PInput, POutput>>
        TransformEvaluator<TransformT> translate(
            TransformHierarchy.Node node, TransformT transform, Class<TransformT> transformClass) {
      //--- determine if node is bounded/unbounded.
      // usually, the input determines if the PCollection to apply the next transformation to
      // is BOUNDED or UNBOUNDED, meaning RDD/DStream.
      Collection<TaggedPValue> pValues;
      if (node.getInputs().isEmpty()) {
        // in case of a PBegin, it's the output.
        pValues = node.getOutputs();
      } else {
        pValues = node.getInputs();
      }
      PCollection.IsBounded isNodeBounded = isBoundedCollection(pValues);
      // translate accordingly.
      LOG.debug("Translating {} as {}", transform, isNodeBounded);
      return isNodeBounded.equals(PCollection.IsBounded.BOUNDED)
          ? translator.translateBounded(transformClass)
              : translator.translateUnbounded(transformClass);
    }

    private PCollection.IsBounded isBoundedCollection(Collection<TaggedPValue> pValues) {
      // anything that is not a PCollection, is BOUNDED.
      // For PCollections:
      // BOUNDED behaves as the Identity Element, BOUNDED + BOUNDED = BOUNDED
      // while BOUNDED + UNBOUNDED = UNBOUNDED.
      PCollection.IsBounded isBounded = PCollection.IsBounded.BOUNDED;
      for (TaggedPValue pValue: pValues) {
        if (pValue.getValue() instanceof PCollection) {
          isBounded = isBounded.and(((PCollection) pValue.getValue()).isBounded());
        } else {
          isBounded = isBounded.and(PCollection.IsBounded.BOUNDED);
        }
      }
      return isBounded;
    }
  }
}

