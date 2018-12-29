$("#menu{{prefix}}{{jobId}} [id^=status{{prefix}}]").each(function(){
    if ($(this).text() == "Not Started"){
        $(this).text("Skipped");
    }
})
$("#pm_overallNumCores{{prefix}}").text("{{totalCores}}");
$("#pm_overallNumExecutors{{prefix}}").text("{{numExecutors}}");

var n = $("#pm_overallProgress{{prefix}}");
n.attr("value", parseInt(n.attr("max")))
