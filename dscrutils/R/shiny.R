#' @title plot results for DSC
#'
#' @description interactive plot for results of DSC
#'
#' @param res
#' @return a shiny plot
#' @export
shiny_plot=function(res, s = "scenario", m = "method"){
  ## Do not make these packages dependency
  ## install them on need only when this function is called
  list.of.packages <- c("ggplot2", "shiny", "dplyr", "rlang")
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  require(shiny)
  require(ggplot2)
  scenario_names = as.character(unique(res[[s]]))
  method_names = as.character(unique(res[[m]]))
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
        res.filter = dplyr::filter(res,rlang::UQ(as.name(s)) %in% input$scen.subset & rlang::UQ(as.name(m)) %in% input$method.subset)
        print(input)
        res.filter$value = res.filter[[input$criteria]]
        ggplot(res.filter, aes_string(m, quote(value), color=m)) +
              geom_boxplot() + facet_grid(as.formula(paste("~",s)))
      })
    }
  )
  shinyApp(ui=ui,server=server)
}
