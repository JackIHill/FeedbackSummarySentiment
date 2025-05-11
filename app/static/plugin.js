

function createDropdown() {
    
    const selectContainer = document.createElement("div");
    selectContainer.classList.add("select-container", "filter-item");

    const selectBox = document.createElement("div");
    selectBox.classList.add("select-box");
    selectBox.id = "selectMonths";

    const input = document.createElement("input");
    input.type = "text";
    input.classList.add("select-text-input");
    input.classList.add("readonly");
    input.readOnly = true;
    input.onclick = function(event) { toggleDropdown(true, event); };
    
    const selectOverlay = document.createElement("div");
    selectOverlay.classList.add("select-overlay");

    const arrowIconWrapper = document.createElement("div");
    arrowIconWrapper.classList.add("arrow-icon-wrapper");
    
    const arrowIcon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    arrowIcon.classList.add("arrow");
    arrowIcon.setAttribute("width", "24");
    arrowIcon.setAttribute("height", "24");
    arrowIcon.setAttribute("viewBox", "0 0 24 24");
    
    const arrowPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
    arrowPath.classList.add("icon-path");
    arrowPath.setAttribute("d", "M16.59 8.59 12 13.17 7.41 8.59 6 10l6 6 6-6z");

    // Create wrapper for dropdown
    const dropDownWrapper = document.createElement("div");
    dropDownWrapper.classList.add("drop-down-wrapper");

    const dropDownContainer = document.createElement("div");
    dropDownContainer.classList.add("drop-down-container", "dates");
    
    const dropDownBody = document.createElement("div");
    dropDownBody.classList.add("drop-down-body", "dates");
    
    selectBox.appendChild(input);
    // arrowIcon.appendChild(arrowPath);
    // arrowIconWrapper.appendChild(arrowIcon);
    // selectOverlay.appendChild(arrowIconWrapper);
    selectBox.appendChild(selectOverlay);
    selectContainer.appendChild(selectBox);

    dropDownContainer.appendChild(dropDownBody);
    dropDownWrapper.appendChild(dropDownContainer);
    selectContainer.appendChild(dropDownWrapper);

    return { selectContainer, input, dropDownBody, dropDownContainer };
}


function addDropDownRows(dropDownBody, typeYearOrMonth, value, index, months) {
    if (index === null) {
        index = value;
    }
    
    var row = document.createElement("div");
    row.classList.add("drop-down-row", "label");
    row.onclick = function(event) { toggleDropdown(false, event); };
    row.setAttribute(`data-${typeYearOrMonth}`, index); 

    var label = document.createElement("label");
    label.setAttribute("for", `${typeYearOrMonth}-${index}`);
    label.classList.add("label");

    var text = document.createElement("span");

    text.classList.add(`${typeYearOrMonth}-label-text`)
    text.textContent = (typeYearOrMonth === 'year') ? value : months[index];

    label.appendChild(text);
    row.appendChild(label);
    dropDownBody.appendChild(row);

    return { label };
}


function isInRange(year, month, minDate, maxDate) {
    let monthStart = new Date(year, month, 1);
    if (minDate && monthStart < minDate) return false;
    if (maxDate && monthStart > maxDate) return false;
    return true;
}


function createOuterWrapper(fp) {
    if (!fp.dateSelectWrapper) {
        fp.dateSelectWrapper = document.createElement("div");
        fp.dateSelectWrapper.classList.add("date-select-wrapper");
    }
}


const monthDropdownPlugin = function () {
    const months = [
        "January", "February", "March", "April", "May", "June", 
        "July", "August", "September", "October", "November", "December"
    ];
    const { selectContainer, input, dropDownBody, dropDownContainer } = createDropdown();


    let selectedMonth = new Date().getMonth();



    var createMonthItems = function (month, year, minDate, maxDate) {
        if (month !== selectedMonth) {
            selectedMonth = month;
        }
        dropDownBody.innerHTML = "";

        
        months.forEach((month, index) => {
            if (isInRange(year, index, minDate, maxDate)) {
                const { label } = addDropDownRows(dropDownBody, 'month', month, index, months);

                if (index === selectedMonth) {
                    label.classList.add("selected");
                }
            }
        });
    };

    return function (fp) {
        let minDate = fp.config.minDate;
        let maxDate = fp.config.maxDate;
        
        
        createOuterWrapper(fp)

        fp.monthSelectContainer = fp._createElement("div", "flatpickr-monthDropdown-months-custom inner-drop-down");
        
        input.placeholder = months[selectedMonth];

        fp.config.onMonthChange.push(function() {
            input.placeholder = months[fp.currentMonth];
        });

        selectContainer.addEventListener('click', function (evt) {

            
            let currentYear = fp.currentYear;
            createMonthItems(fp.currentMonth, currentYear, minDate, maxDate);

            if (evt.target && (
                evt.target.classList.contains("label") ||
                evt.target.classList.contains("month-label-text")
                )
            ) {
                var month = evt.target.closest('.drop-down-row').getAttribute("data-month");
                selectedMonth = month;

                var currentMonth = fp.currentMonth;
                var monthDifference = month - currentMonth;
                fp.changeMonth(monthDifference);

                dropDownBody.querySelectorAll('.drop-down-row').forEach(item => item.classList.remove('selected'));
                evt.target.closest('.drop-down-row').classList.add('selected');
            }
            
            createMonthItems(selectedMonth-0, currentYear, minDate, maxDate);
        });

        fp.monthSelectContainer.appendChild(selectContainer);
        fp.dateSelectWrapper.appendChild(fp.monthSelectContainer);

        return {
            onReady: function onReady() {
                var name = fp.monthNav.className;                
                const monthInputCollection = fp.calendarContainer.getElementsByClassName(name);
                const el = monthInputCollection[0];
            
                if (!fp.dateSelectWrapper.inserted) {
                    el.parentNode.insertBefore(fp.dateSelectWrapper, el.parentNode.firstChild);
                    fp.dateSelectWrapper.inserted = true;
                }



                // console.log(rangeButtons)

            },
        };
    };
};

window.monthDropdownPlugin = monthDropdownPlugin;


const yearDropdownPlugin = function (pluginConfig) {
    var defaultConfig = {
        yearStart: 100,
        yearEnd: 2,
    };

    var config = { ...defaultConfig, ...pluginConfig };

    var currYear = new Date().getFullYear();

    const { selectContainer, input, dropDownBody } = createDropdown();

    let selectedYear = currYear
    var createYearItems = function (year, month, minDate, maxDate) {
        dropDownBody.innerHTML = "";

        if (year !== selectedYear) {
            selectedYear = year;
        }

        let minYear = minDate ? minDate.getFullYear() : currYear - config.yearStart;
        let maxYear = maxDate ? maxDate.getFullYear() : currYear + config.yearEnd;

        for (let year = minYear; year <= maxYear; year++) {
            const { label } = addDropDownRows(dropDownBody, 'year', year, year);

            if (year === selectedYear) {
                label.classList.add("selected");
                 }
        }
    };

    return function (fp) {
        let minDate = fp.config.minDate;
        let maxDate = fp.config.maxDate;

        createOuterWrapper(fp)

        fp.yearSelectContainer = fp._createElement("div", "flatpickr-yearDropdown-years-custom inner-drop-down");

        input.placeholder = currYear;

        fp.config.onYearChange.push(function () {
            input.placeholder = fp.currentYear;
        });

        selectContainer.addEventListener("click", function (evt) {
            // console.log(fp.currentYear)
            createYearItems(fp.currentYear, fp.currentMonth, minDate, maxDate);

            if (
                evt.target &&
                (evt.target.classList.contains("label") || evt.target.classList.contains("year-label-text"))
            ) {
                var year = parseInt(evt.target.closest(".drop-down-row").getAttribute("data-year"), 10);

                // Determine if the selected year is outside minDate or maxDate range, if so set to the min/max date month/year.
                if (minDate && year === minDate.getFullYear() && fp.currentMonth < minDate.getMonth()) {
                    fp.changeYear(year);
                    fp.changeMonth(minDate.getMonth() - fp.currentMonth);
                } 
                else if (maxDate && year === maxDate.getFullYear() && fp.currentMonth > maxDate.getMonth()) {
                    fp.changeYear(year);
                    fp.changeMonth(maxDate.getMonth() - fp.currentMonth);
                } 
                else {
                    fp.changeYear(year);
                }

                dropDownBody.querySelectorAll(".drop-down-row").forEach((item) => item.classList.remove("selected"));
                evt.target.closest(".drop-down-row").classList.add("selected");

                createYearItems(year-0, null, minDate, maxDate);

            }
        });
        fp.yearSelectContainer.appendChild(selectContainer);
        fp.dateSelectWrapper.appendChild(fp.yearSelectContainer);


        return {
            onReady: function onReady() {
                var name = fp.monthNav.className;            
                const yearInputCollection = fp.calendarContainer.getElementsByClassName(name);
                const el = yearInputCollection[0];

                // Insert wrapper only once
                if (!fp.dateSelectWrapper.inserted) {
                    el.parentNode.insertBefore(fp.dateSelectWrapper, el.parentNode.firstChild);
                    fp.dateSelectWrapper.inserted = true;
                }
            },
        };
    };
};

window.yearDropdownPlugin = yearDropdownPlugin;

let isManualSelection = true
const updateSelectedDateRangePlugin = function () {
    return function(fp) {
      return {
        onReady: function onReady() {         
          const rangeButtons = document.querySelectorAll('.persistent-btn.dates')

          rangeButtons.forEach(button => {
              button.addEventListener("click", () =>  {
                isManualSelection = false
                let start = new Date(fp.config.minDate);
                const end = new Date(fp.config.maxDate);
                
                // button ids contain the durations (1-month, 3-month etc.)
                if (button.id !== 'default-months') {
                    const duration = parseInt(button.id, 10);
                    let newStart = new Date(end);
                    newStart.setMonth(newStart.getMonth() - duration);
                    
                    // start date = operator minDate if outside of the duration.
                    if (newStart < fp.config.minDate) {
                        start = new Date(fp.config.minDate);
                    } else {
                        start = newStart;
                    }
                }
                // Deactivate previous selection before setting the new one
                if (pendingDatePreselectButton && pendingDatePreselectButton !== button) {
                    deactivateButtonPersistence(pendingDatePreselectButton);
                }
            
                // Set the new active button and range
                pendingDatePreselectButton = button;
                activateButtonPersistence(button);
            
                fp.setDate([start, end], true); // triggers onChange and updates pendingSelectedDates
                isManualSelection = true
              })
          })

        },
      };
    };
  }

window.updateSelectedDateRangePlugin = updateSelectedDateRangePlugin;