$("#status{{prefix}}{{stageId}}").text("{{status}}")
$("#pm_overallNumCores{{prefix}}").text("{{totalCores}}");
$("#pm_overallNumExecutors{{prefix}}").text("{{numExecutors}}");

{% if host%}
$("#host{{prefix}}{{stageId}}").text("{{host}}")
{%endif%}
