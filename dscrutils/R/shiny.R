#' @title plot results for DSC
#'
#' @description interactive plot for results of DSC. Particularly tailored
#' for the simple "simulate-analyze-score" framework. It plots boxplots of a score
#' for each analyze module, facetting by the simulate module.
#'
#' @param res a dataframe containing results (eg from )
#' @param simulate_col The name of the column that distinguishes simulate modules
#' @param analyze_col The name of the column that distinguishes analyze modules
#' @param score_col The name of the column that distinguishes score modules
#' @param scales parameter to be passed to ggplot2::facet_wrap affects y axis scaling
#' @return a shiny plot
#' @export
shiny_plot=function(res, simulate_col = "simulate", analyze_col = "analyze", score_col = "score", scales="free"){
  ## Do not make these packages dependency
  ## install them on need only when this function is called
  list.of.packages <- c("ggplot2", "shiny", "dplyr", "rlang")
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  require(shiny)
  require(ggplot2)
  scenario_names = as.character(unique(res[[simulate_col]]))
  method_names = as.character(unique(res[[analyze_col]]))
  score_names = as.character(unique(res[[score_col]]))
  
  numeric_criteria = names(res)[unlist(lapply(res,is.numeric))]
  numeric_criteria = numeric_criteria[numeric_criteria!="DSC"]
  
  ui=shinyUI(pageWithSidebar(
    headerPanel('DSC Results'),
    sidebarPanel(
      checkboxGroupInput("scen.subset", "Choose Scenarios",
                                               choices  = scenario_names,
                                               selected = scenario_names),
      checkboxGroupInput("method.subset", "Choose Methods",
                            choices  = method_names,
                            selected = method_names),
      selectInput("score", "Choose Score",
                         choices  = score_names,
                         selected = score_names[1]),
      selectInput("criteria", "Choose Criteria",
                      choices  = numeric_criteria,
                      selected = numeric_criteria[1])

    ),
    mainPanel(
      plotOutput('plot1')
    )
  ))

  server = shinyServer(
    function(input, output, session) {
      output$plot1 <- renderPlot({
        res.filter = 
          dplyr::filter(res,rlang::UQ(as.name(simulate_col)) %in% input$scen.subset & 
                      rlang::UQ(as.name(analyze_col)) %in% input$method.subset & rlang::UQ(as.name(score_col)) %in% input$score)
        print(input)
        res.filter$value = res.filter[[input$criteria]]
        ggplot(res.filter, aes_string(analyze_col, quote(value), color=analyze_col)) +
              geom_boxplot() + facet_wrap(as.formula(paste("~",simulate_col)),scales=scales)
      })
    }
  )
  shinyApp(ui=ui,server=server)
}
