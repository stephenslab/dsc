#' @title plot results for DSC
#'
#' @description interactive plot for results of DSC. Particularly
#' tailored for the simple "simulate-analyze-score" framework. It
#' plots boxplots of a score for each analyze module, facetting by the
#' simulate module.
#'
#' @param res a dataframe containing results (eg from )
#' 
#' @param simulate_col The name of the column that distinguishes
#'  simulate modules.
#' 
#' @param analyze_col The name of the column that distinguishes
#'   analyze modules.
#' 
#' @param score_col The name of the column that distinguishes score
#'   modules.
#' 
#' @param scales parameter to be passed to ggplot2::facet_wrap affects
#'   y axis scaling.
#' 
#' @return A shiny plot.
#'
#' @importFrom stats as.formula
#' 
#' @export
#'
shiny_plot = function(res, simulate_col = "simulate",
    analyze_col = "analyze", score_col = "score", scales="free") {

  # Check that the additional suggested packages are available. If
  # not, throw an error.
  error.msg <- paste("shiny_plot requires the following packages:",
                     "ggplot2, shiny, dplyr and rlang")
  if (!requireNamespace("ggplot2",quietly = TRUE))
    stop(error.msg)
  if (!requireNamespace("shiny",quietly = TRUE))
    stop(error.msg)
  if (!requireNamespace("dplyr",quietly = TRUE))
    stop(error.msg)
  if (!requireNamespace("rlang",quietly = TRUE))
    stop(error.msg)

  scenario_names <- as.character(unique(res[[simulate_col]]))
  method_names   <- as.character(unique(res[[analyze_col]]))
  score_names    <- as.character(unique(res[[score_col]]))
  
  numeric_criteria <- names(res)[unlist(lapply(res,is.numeric))]
  numeric_criteria <- numeric_criteria[numeric_criteria != "DSC"]
  
  ui <- shiny::shinyUI(shiny::pageWithSidebar(
    shiny::headerPanel('DSC Results'),
    shiny::sidebarPanel(
      shiny::checkboxGroupInput("scen.subset", "Choose Scenarios",
                                choices  = scenario_names,
                                selected = scenario_names),
      shiny::checkboxGroupInput("method.subset", "Choose Methods",
                                choices  = method_names,
                                selected = method_names),
      shiny::selectInput("score", "Choose Score",
                         choices  = score_names,
                         selected = score_names[1]),
      shiny::selectInput("criteria", "Choose Criteria",
                         choices  = numeric_criteria,
                         selected = numeric_criteria[1])

    ),
    shiny::mainPanel(
      shiny::plotOutput('plot1')
    )
  ))

  server = shiny::shinyServer(
    function(input, output, session) {
      output$plot1 <- shiny::renderPlot({
        res.filter = 
          dplyr::filter(res,rlang::UQ(as.name(simulate_col)) %in%
                        input$scen.subset & 
                        rlang::UQ(as.name(analyze_col)) %in%
                        input$method.subset &
                        rlang::UQ(as.name(score_col)) %in% input$score)
        print(input)
        res.filter$value = res.filter[[input$criteria]]
        ggplot2::ggplot(res.filter,
                        ggplot2::aes_string(analyze_col, quote(value),
                                            color = analyze_col)) +
          ggplot2::geom_boxplot() +
            ggplot2::facet_wrap(as.formula(paste("~",simulate_col)),
                                scales = scales)
      })
    }
  )
  shiny::shinyApp(ui = ui,server = server)
}
