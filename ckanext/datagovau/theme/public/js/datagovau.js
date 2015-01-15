window.onload = windowOnLoad()

function windowOnLoad () {
  addAltToAvatar();
  addTextToI();
  addTitleToSearch();
  insertRequiredNoteBeforeForm();
  textAfterDropdown();
}

function addAltToAvatar () {
  var name = $('span.username').text();
  var user = $('img.gravatar');
  _updateAttribute(user, 'alt', (name || 'User' ) + ' profile image')
}

function addTitleToSearch () {
  var search = $('input.search');
  _updateAttribute(search, 'title', 'Search:')
  
  search = $('input#field-sitewide-search');
  _updateAttribute(search, 'title', 'Sitewidth search:')
}

function addTextToI () {
  _addReaderTextToButtons('i.icon-remove', 'Remove item');
  _addReaderTextToButtons('form.site-search  i.icon-search', 'Search');
}

function insertRequiredNoteBeforeForm () {
  $('.control-group').first().before($('.control-required-message'));
}

function textAfterDropdown () {
  var icon = $('.resource-item .dropdown-toggle i');
  icon.after($('<span>').addClass('visually-hidden').text(' show below'))
  $('.resource-item .dropdown').on('click', _onDropClick)
}

function _updateAttribute (element, attr, value) {
  element.attr(attr, element.attr(attr) || value)
}

function _addReaderTextToButtons(selector, text){
  var self = $(selector);
  $(selector).html('<img src="" alt="'+text+'"/>');
  if ( self.next().is('span') ) return;
  self.after($('<span>').addClass('visually-hidden').text(text));
}

function _onDropClick (event) {
  var self = $(this);
  if (self.hasClass('open')){
    $(event.target).find('.visually-hidden').text(' show below');
  } else {
    $(event.target).find('.visually-hidden').text(' hide below');
  }
}