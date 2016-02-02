# Purpose

The goal of the `dsc2` project is much the same as its R package
predecessor, `dscr`:

[Dynamic Statistical Comparisons in R]
(https://github.com/stephens999/dscr/)

Briefly, when performing a benchmarking study of statistical
methodologies, one sees a "table of scores" as a first-class object
and wants some software tooling around tables of scores:

1. To separate the generation of benchmarking test cases ("scenarios")
   from the statistical tools under test ("methods") from the
   definition of figures of merit ("scores") through consistent
   interfaces.
2. To automate the execution of the Cartesian product between
   scenarios, methods, and scores, both on a serial local host and in
   parallel using scientific cluster resources.
3. To think about file organization as little as possible by having it
   under the hood of a disciplined framework.
4. To cache intermediate results of computational pipelines for
   efficiency.
5. To easily clear portions of an existing collection of tables of
   scores while developing and debugging it.
6. To easily share the table of scores on a collaborative platform
   (e.g., GitHub) either with the cached results of intermediate
   computation (for auditability) or without (for space efficiency
   when data sets or intermediate results become large).
7. To contribute to an existing table of scores without needing to
   replicate the computations leading up to the old scores.

# Lessons Learned

The `dscr` package has been in production since early 2015 in the
Stephens lab and among some of our friends; we've learned some lessons
in this time.

1. Ad hoc dependency management is difficult to get correct and even
   more difficult to maintain when entangled with other aspects of the
   tooling, and a battle-tested directed acyclic graph (DAG) execution
   engine should be relied upon from the outset in `dsc2`.
2. Shoehorning the user into any prescribed number of steps in their
   workflow is counterproductive, as users often want to re-use
   expensive preprocessed results between scenarios, and fighting or
   misunderstanding these prescriptions sometimes results in software
   design antipatterns in user code.
3. It is hard to understand how to extend an existing DSC without
   executable documentation (e.g., tests) for what scenarios are
   expected to present to methods and for what methods are expected to
   present to scores.
4. Good cluster abstractions, allowing for parallel computation of a
   table of scores, are rare. `BatchJobs` is one such abstraction in
   the R ecosystem. Building `dsc2` around a good cluster abstraction
   will be a big help.
5. Declarative semantics for specification of a table of scores in a
   single location will be easier to comprehend than imperative
   semantics (such as `add_scenario`, `add_method`, etc., in `dscr`)
   spread out through multiple files.
6. Including all data and intermediate computation in a DSC can make
   it unwieldy to share with a collaborator. We need to figure out how
   to include enough partial information about a DSC to keep it
   extensible but still reasonable in size for practical cases.
7. We use a lot of R, but even so, some methods have been executed in
   other languages or systems (such as executable C++ binaries or
   Matlab scripts). We can be flexible in terms of programming
   language and runtime environment as long as we can "shell out" to R
   and other languages via a UNIX-like operating system.
