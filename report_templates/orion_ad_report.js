$(document).ready(function () {

    if ($("#fmeaTableDiv tbody td").length == 0){
        $(".section-fmea").hide();
    }

    if ($("#feedbackTableDiv tbody td").length == 0){
        $(".section-pass-feedback").hide();
    }

    displayFeedbackForm();
    $("#resultPlots div").hide();
    $("#resultPlots").children().slice(0,3).show().children().show().children().show();


	$('.section-result table').DataTable({
        "aLengthMenu": [[1, 5, 10, -1], [1, 5, 10, "All"]],
        "iDisplayLength": -1,
		"order": [[ 1, "asc" ]]
    });

    $("#resultTableDiv tbody tr").on("click", function () {
        var stepName = $(this).find("td:first").html();
		var stepType = $(this).find("td:nth-child(2)").html();
		var feature = $(this).find("td:nth-child(3)").html();
		var context_value = $(this).find("td:nth-child(4)").html();
        var chart_id = (stepType + "::" + stepName + "::" + feature + "::" + context_value).replace(/[^a-z0-9]/gi,'').toLowerCase();
        console.log(chart_id)
        if ($(this).hasClass("clicked")){
            $(this).removeClass("clicked");
                $("#resultPlots div").hide();
                $("#resultPlots").children().slice(0,3).show().children().show().children().show();
            }
        else {
            $(".clicked").removeClass("clicked");
            $(this).addClass("clicked");
            $("#resultPlots div").hide();
            $("#" + chart_id).show();
            $("#" + chart_id).children().show();
            $("#" + chart_id).children().children().show();
        }

  });

    $('#fb_cat label input[type=radio]').change(function () {
        displayFeedbackForm();
    });
});

function displayFeedbackForm() {
    var optionValue = $('#fb_cat label input:checked').val();
    $('#fb_cat label').has("input:checked").css("color", "blue");
    $('#fb_cat label').has("input:not(:checked)").css("color", "black");
    var select_rootcause = $("#flaggedRootCause");
    select_rootcause.find('option').remove();
    var step = [], step_type = [], cause_type = [], root_case = [];
    $('#resultTableDiv tbody tr td:nth-child(1)').each(function () {
        step.push($(this).text());
    });
    $('#resultTableDiv tbody tr td:nth-child(2)').each(function () {
        step_type.push($(this).text());
    });
    $('#resultTableDiv tbody tr td:nth-child(3)').each(function () {
        cause_type.push($(this).text());
    });
    $('#resultTableDiv tbody tr td:nth-child(4)').each(function () {
        root_case.push($(this).text());
    });

    for (var i = 0; i < step.length; i++) {
        root_cause_option = step[i] + "::" + step_type[i] + "::" + cause_type[i] + "::" + root_case[i];
        var option_step = new Option(root_cause_option, root_cause_option);
        select_rootcause.append($(option_step));
    }

    if (optionValue == 0) {
        $('.actual').hide();
        $('.flagged').show();
    }
    else if (optionValue == 1) {
        $('.actual').show();
        $('.flagged').hide();
    }
    else {
        $('.actual').hide();
        $('.flagged').hide();
    }
}