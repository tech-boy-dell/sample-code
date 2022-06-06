package com.aem.reg.core.services.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aem.reg.core.commons.CommonSearchUtil;
import com.aem.reg.core.commons.PageUtils;
import com.aem.reg.core.constants.RegportalConstants;
import com.aem.reg.core.services.FulltextSearchService;
import com.day.cq.dam.api.Asset;
import com.day.cq.tagging.TagManager;
import com.day.cq.wcm.api.Page;
import com.day.cq.wcm.api.PageManager;


@Component(service = FulltextSearchService.class, immediate = true, property = {
		Constants.SERVICE_DESCRIPTION + "= Fulltext Search Service",
		JobConsumer.PROPERTY_TOPICS + "=" + FulltextSearchServiceImpl.PATH })
public class FulltextSearchServiceImpl implements FulltextSearchService {

	public static final String PATH = "com/aem/reg/core/services/iml/FulltextSearchServiceImpl";
	private static final Logger LOG = LoggerFactory.getLogger(FulltextSearchServiceImpl.class);

	/**
	 * Gets the full text search result.
	 */
	
	@Override
	public JSONObject getFullTextSearchResult(SlingHttpServletRequest req, ResourceResolver resourceResolver,
			String keyword, String group, String folders) {
		List<HashMap<Resource, String>> hits = new ArrayList<>();
		JSONObject jsonResultObj = new JSONObject();
		try {
			LOG.info("fulltext search keyword={}", keyword);
			Iterator<Resource> allResult = getFullTextMatchingResult(resourceResolver, keyword, group, folders);
			if (null != allResult) {
				Set<Resource> searchresultList = new LinkedHashSet<>();
				while (allResult.hasNext()) {
					Resource searchRes = allResult.next();
					searchresultList.add(searchRes);
				}
				LOG.info("First Query Result Size={} ", searchresultList.size());
				getPageResults(group, folders, resourceResolver, hits, searchresultList);
				LOG.info("Total Page Result Size={} ", hits.size());
				if (!hits.isEmpty()) {
					Iterator<HashMap<Resource, String>> resultIte = hits.iterator();
					jsonResultObj = getJsonResult(resultIte, resourceResolver, req, group);
				} else {
					LOG.info("hits is null or size is zero");
				}
			} else {
				LOG.info("No Result Found in getFullTextMatchingResult.");
			}

		} catch (ParseException e) {
			LOG.error("Parse exception ", e);
		} catch (RepositoryException e) {
			LOG.error("throw repository exception", e);
		} catch (JSONException e) {
			LOG.error("throw JSONException exception", e);
		}

		return jsonResultObj;
	}


	/**
	 * Gets the search results after linking the assets to the pages.
	 */
	public List<HashMap<Resource, String>> getPageSearchResult(ResourceResolver resourceResolver, String path, String pageSearchPath,
			String blockedPath) {
		LOG.info("getListOfPages started.  pageSearchPath={}", pageSearchPath);
		List<HashMap<Resource, String>> pageResourceList = new ArrayList<HashMap<Resource, String>>();
		LOG.info("PDF Path={}", path);
		String pageQuery = "";

		if (Objects.nonNull(blockedPath)) {
			pageQuery = "SELECT * FROM [cq:Page] AS s WHERE CONTAINS(s.*, '" + path + "') AND [jcr:path] like '"
					+ pageSearchPath + "%' AND [jcr:path] NOT like '/%/" + blockedPath + "%'";
		} else {
			pageQuery = "SELECT * FROM [cq:Page] AS s WHERE CONTAINS(s.*, '" + path + "') AND [jcr:path] like '"
					+ pageSearchPath + "%'";
		}

		LOG.info("pageQuery={}", pageQuery);

		Iterator<Resource> pageSearchResult = resourceResolver.findResources(pageQuery, "sql");
		LOG.info("found pages for PDF ={}", path);
		while (pageSearchResult.hasNext()) {
			HashMap<Resource, String> pageAndPath= new HashMap<Resource, String>();
			pageAndPath.put(pageSearchResult.next(), path);
			pageResourceList.add(pageAndPath);

		}
		LOG.info("getListOfPages end. pageResourceList size={} ", pageResourceList.size());
		return pageResourceList;
	}

	/**
	 * Gets the search results after de-duplication and display those results in an array based on group and folder param.
	 */
	public JSONObject getJsonResult(Iterator<HashMap<Resource, String>> result, ResourceResolver resourceResolver,
			SlingHttpServletRequest req, String groupValue) throws RepositoryException, JSONException {
		if (StringUtils.isEmpty(groupValue)) {
			groupValue = "all";
		}
		JSONObject finalResult = new JSONObject();
		PageManager pageManager = resourceResolver.adaptTo(PageManager.class);
		TagManager tagManager = resourceResolver.adaptTo(TagManager.class);
		String dashPagePath = "/content/regportal/dashboard/";
		String mode = PageUtils.getMode(req);
		JSONArray allResult = new JSONArray();

		JSONObject responseObject = new JSONObject();

		JSONObject easyGroup = new JSONObject();
		JSONObject cesGroup = new JSONObject();
		JSONArray othersArr = new JSONArray();

		if ("ces".equalsIgnoreCase(groupValue)) {
			responseObject.put("ces", cesGroup);
			responseObject.put("others", othersArr);
		} else if ("EasyGroup".equalsIgnoreCase(groupValue)) {
			responseObject.put("easyGroup", easyGroup);
			responseObject.put("others", othersArr);
		} else {
			responseObject.put("easyGroup", easyGroup);
			responseObject.put("ces", cesGroup);
			responseObject.put("others", othersArr);
		}

		JSONArray easyannouncementsArr = new JSONArray();
		JSONArray easyknownissuesArr = new JSONArray();
		JSONArray easyreleasesArr = new JSONArray();
		JSONArray easydocumentationanddetailsArr = new JSONArray();

		JSONArray cesannouncementsArr = new JSONArray();
		JSONArray cesknownissuesArr = new JSONArray();
		JSONArray cesreleasesArr = new JSONArray();
		JSONArray ceslcdreleasesArr = new JSONArray();
		JSONArray cesdocumentationanddetailsArr = new JSONArray();

		JSONArray regulatoryinsightArrCes = new JSONArray();
		JSONArray regulatoryinsightArrEasy = new JSONArray();

		easyGroup.put("easyannouncements", easyannouncementsArr);
		easyGroup.put("easyknownissues", easyknownissuesArr);
		easyGroup.put("easyreleases", easyreleasesArr);
		easyGroup.put("easydocumentationanddetails", easydocumentationanddetailsArr);
		easyGroup.put("regulatoryinsight", regulatoryinsightArrEasy);

		cesGroup.put("cesannouncements", cesannouncementsArr);
		cesGroup.put("cesknownissues", cesknownissuesArr);
		cesGroup.put("cesreleases", cesreleasesArr);
		cesGroup.put("ceslcdreleases", ceslcdreleasesArr);
		cesGroup.put("cesdocumentationanddetails", cesdocumentationanddetailsArr);
		cesGroup.put("regulatoryinsight", regulatoryinsightArrCes);

		/**
		 * Search servlet results return title, size, path, postedDate, relevance, grp, and tags for Pages / Downloadable assets
		 */

		int count = 1;
		Set<String> pagePaths = new LinkedHashSet<>();
		while (result.hasNext()) {
			HashMap<Resource, String> hMap= result.next();
			Resource resource = hMap.entrySet().stream().findFirst().get().getKey();
			JSONObject object = new JSONObject();
			String title = "";
			String size = "";
			String path = "";
			String postedDate = "";
			int rindex = 0;
			String grp = "";
			JSONArray jsonTagArray = new JSONArray();
			Node node = resource.adaptTo(Node.class);
			if (node != null && isNotDuplicate(node.getPath(), pagePaths)
					&& !node.getPath().contains("regulatory-insights/")) {
				String nodePath = node.getPath();
				pagePaths.add(nodePath);
				try {
					if (nodePath.startsWith(dashPagePath)) {
						LOG.info("PAGE");
						path = nodePath + ".html";
						Page page = pageManager.getPage(nodePath);
						if (Objects.nonNull(page)) {
							postedDate = PageUtils.getPublishDate(page, mode);
							rindex = count;
							grp = CommonSearchUtil.getGroupName(nodePath);
							if (grp.length() > 0) {
								jsonTagArray = PageUtils.getTags(resource.getChild("jcr:content"), tagManager,
										grp.toLowerCase(Locale.ENGLISH));
							}
						} else {
							LOG.error("page not found");
						}
						Resource jcrResource = resource.getChild("jcr:content");
						if (jcrResource != null) {
							LOG.info("PAGE JCR");
							ValueMap valueMap = jcrResource.adaptTo(ValueMap.class);
							if (valueMap != null) {
								title = valueMap.get("jcr:title", "");
							} else {
								LOG.info("valueMap is null");
							}
						} else {
							LOG.info("jcrResource is null");
						}

						object.put(RegportalConstants.TYPE, RegportalConstants.PAGE);
						object.put("title", title);
						object.put("path", path);
						object.put("postedDate", postedDate);
						object.put("rindex", rindex);
						object.put("tags", jsonTagArray);

						if ("easygroup".equals(grp.toLowerCase(Locale.ENGLISH))) {
							if (path.contains("easyannouncements")) {
								easyannouncementsArr.put(object);
							} else if (path.contains("easyknownissues")) {
								easyknownissuesArr.put(object);
							} else if (path.contains("easyreleases")) {
								easyreleasesArr.put(object);
							} else if (path.contains("documentationanddetails") || path.contains("easyproducts")) {
								easydocumentationanddetailsArr.put(object);
							} else {
								othersArr.put(object);
							}
						} else if ("ces".equals(grp.toLowerCase(Locale.ENGLISH))) {
							if (path.contains("cesannouncements")) {
								cesannouncementsArr.put(object);
							} else if (path.contains("cesknownissues")) {
								cesknownissuesArr.put(object);
							} else if (path.contains("cesreleases") || path.contains("ces-release-schedule")) {
								cesreleasesArr.put(object);
							} else if (path.contains("ces-lcd-releases")) {
								ceslcdreleasesArr.put(object);
							} else if (path.contains("documentationanddetails")) {
								cesdocumentationanddetailsArr.put(object);
							} else {
								othersArr.put(object);
							}
						} else if (path.contains("regulatory-insights")) {
							
							JSONArray jsonTagArrayEasy = new JSONArray();
							jsonTagArrayEasy.put("easygroup/allpaymentsystems/allproducts");
							JSONArray jsonTagArrayCes = new JSONArray();
							jsonTagArrayCes.put("claimseditsystem/alllinesofbusiness/allclaimtypes");
							if (hMap.entrySet().stream().findFirst().get().getValue().startsWith("/content/dam/regportal/api/regulatory-insights/easygroup")) {
								regulatoryinsightArrEasy.put(object.put("tags", jsonTagArrayEasy));
							} else if(hMap.entrySet().stream().findFirst().get().getValue().startsWith("/content/dam/regportal/api/regulatory-insights/ces")){
								JSONObject copyObject = new JSONObject();
								copyObject.put(RegportalConstants.TYPE, object.get(RegportalConstants.TYPE));
								copyObject.put("title", object.get("title"));
								copyObject.put("path", object.get("path"));
								copyObject.put("postedDate", object.get("postedDate"));
								copyObject.put("rindex", object.get("rindex"));
								copyObject.put("tags", jsonTagArrayCes);
								regulatoryinsightArrCes.put(copyObject);
							}
						} else {
							othersArr.put(object);
						}
						count++;
					} else if (node.getPath().startsWith("/content/dam")) {
						path = node.getPath();
						LOG.info("DAM.  PATH={} ", path);
						Resource metaDataRes = resourceResolver.getResource(path + "/jcr:content/metadata");
						if (metaDataRes != null) {
							ValueMap metaDatavm = metaDataRes.getValueMap();
							Long longSize = metaDatavm.get("dam:size", Long.class);
							LOG.info("DOC SIZE1={} ", longSize);
							if (null != longSize && longSize > 0) {
								size = CommonSearchUtil.getReadableFileSize(longSize);
							}
							Resource damContentRes = metaDataRes.getParent();
							postedDate = PageUtils.getPublishDate(damContentRes, mode);
						} else {
							LOG.info("DAM metadata not found. path={} ", path);
						}
						Asset assetNode = resource.adaptTo(Asset.class);
						if (assetNode != null) {
							if (assetNode.getMetadataValue("dc:title") != null) {
								title = assetNode.getMetadataValue("dc:title");
							} else {
								title = assetNode.getName();
								LOG.info("Title is not available in metadata node.");
							}

						} else {
							LOG.info("jcr:content node is not available");
						}
						rindex = count;
						grp = CommonSearchUtil.getDamGroupName(path);
						jsonTagArray = CommonSearchUtil.getDamTag(path, resourceResolver, tagManager, grp);
						object.put(RegportalConstants.TYPE, RegportalConstants.DOWNLOAD);
						object.put("title", title);
						object.put("path", path);
						object.put(RegportalConstants.SIZE, size);
						object.put("postedDate", postedDate);
						object.put("rindex", rindex);
						object.put("tags", jsonTagArray);
						if ("EasyGroup".equals(grp)) {
							if (path.contains("announcements")) {
								easyannouncementsArr.put(object);
							} else if (path.contains("knownissues")) {
								easyknownissuesArr.put(object);
							} else if (path.contains("releases")) {
								easyreleasesArr.put(object);
							} else if (path.contains("documentationanddetails") || path.contains("products")) {
								easydocumentationanddetailsArr.put(object);
							} else {
								othersArr.put(object);
							}
						} else if ("Ces".equals(grp)) {
							if (path.contains("announcements")) {
								cesannouncementsArr.put(object);
							} else if (path.contains("knownissues")) {
								cesknownissuesArr.put(object);
							} else if (path.contains("releases")) {
								cesreleasesArr.put(object);
							} else if (path.contains("ces-lcd-releases")) {
								ceslcdreleasesArr.put(object);
							} else if (path.contains("documentationanddetails")) {
								cesdocumentationanddetailsArr.put(object);
							} else {
								othersArr.put(object);
							}
						} else if (path.contains("regulatory-insights")) {
							JSONArray jsonTagArrayEasy = new JSONArray();
							jsonTagArrayEasy.put("easygroup/allpaymentsystems/allproducts");
							JSONArray jsonTagArrayCes = new JSONArray();
							jsonTagArrayCes.put("claimseditsystem/alllinesofbusiness/allclaimtypes");
							regulatoryinsightArrEasy.put(object.put("tags", jsonTagArrayEasy));
							regulatoryinsightArrCes.put(object.put("tags", jsonTagArrayCes));
						} else {
							othersArr.put(object);
						}

						count++;
					} else {
						LOG.error("unsearchable node found TYPE={}", node.getProperty("jcr:primaryType"));
					}
				} catch (RepositoryException e) {
					LOG.error("RepositoryException error: ", e);
				}

			}
			if (count == 2000) {
				break;
			}
		}
		allResult.put(responseObject);
		finalResult.put("allResult", allResult);
		finalResult.put("count", count);
		return finalResult;
	}
	
	private boolean isNotDuplicate(String path, Set<String> pagePaths) {
		boolean result = false;
		if (path.startsWith("/content/regportal/dashboard/regulatory-insights")) {
			result = true;
		} else {
			result = pagePaths.contains(path) ? false : true;
		}
		return result;
	}

	/**
	 * Gets the search results from Pages based on their group values ces, easy, &  all. further filtering of search results are done based on the Parent pages
	 */
	private void getPageResults(String group, String folders, ResourceResolver resourceResolver, List<HashMap<Resource, String>> hits,
			Set<Resource> searchresult) {
		String basePath = "/content/regportal/dashboard/";
		String cesPageBaseFolder = basePath + "ces";
		ArrayList<String> listOfPageFolder = new ArrayList<>(3);
		listOfPageFolder.add("announcements");
		listOfPageFolder.add("knownissues");
		listOfPageFolder.add("releases");
		listOfPageFolder.add("products");
		listOfPageFolder.add(RegportalConstants.LCD_RELEASES);
		listOfPageFolder.add("documentationanddetails");
		listOfPageFolder.add(RegportalConstants.REGULATORY_INSIGHT);

		if (Objects.nonNull(group)) {
			LOG.info("fulltext page search group={} ", group);
			if (Objects.nonNull(folders)) {
				LOG.info("fulltext search pages folders={} ", folders);
				String folderName = "";
				String[] folderArr = CommonSearchUtil.getFolderList(folders);
				for (int i = 0; i < folderArr.length; i++) {
					folderName = folderArr[i].trim();
					LOG.info("Folder Name={} ", folderName);
					if (listOfPageFolder.contains(folderName)) {
						StringBuilder regSearchPath = new StringBuilder("");
						StringBuilder egSearchPath = new StringBuilder("");
						StringBuilder cesSearchPath = new StringBuilder("");
						if (RegportalConstants.REGULATORY_INSIGHT.equals(folderName)) {
							regSearchPath.append(basePath).append(folderName);
						} else {
							if (RegportalConstants.LCD_RELEASES.equals(folderName)) {
								cesSearchPath.append(cesPageBaseFolder).append("-").append(folderName);
							} else {
								cesSearchPath.append(cesPageBaseFolder).append(folderName);
							}
						}

						if (!"knownissues".equals(folderName)) {
							egSearchPath.append(egSearchPath).append(RegportalConstants.FORWARD_SLASH);
							cesSearchPath.append(cesSearchPath).append(RegportalConstants.FORWARD_SLASH);
						}

						if ("all".equals(group)) {
							LOG.info("WITHIN ALL");
							for (Resource resource : searchresult) {
								String path = resource.getPath();
								if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
									if (path.startsWith(RegportalConstants.DAM_CES_DOCUMENTATION)
											|| path.startsWith(RegportalConstants.DAM_EASYGROUP_USERGUIDE)) {
										hits.add(getHashMapObj(resource));
									} else {
										if (RegportalConstants.REGULATORY_INSIGHT.equals(folderName)) {
											hits.addAll(getPageSearchResult(resourceResolver, path,
													regSearchPath.toString(), null));
										} else {
											hits.addAll(getPageSearchResult(resourceResolver, path,
													egSearchPath.toString(), null));
											hits.addAll(getPageSearchResult(resourceResolver, path,
													cesSearchPath.toString(), null));
										}
									}
								} else {
									hits.add(getHashMapObj(resource));
								}
							}
						} else if ("easygroup".equals(group)) {
							LOG.info("WITHIN easygroup");
							for (Resource resource : searchresult) {
								String path = resource.getPath();
								if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
									if (path.startsWith(RegportalConstants.DAM_EASYGROUP_USERGUIDE)) {
										hits.add(getHashMapObj(resource));
									} else {
										if (RegportalConstants.REGULATORY_INSIGHT.equals(folderName)) {
											hits.addAll(getPageSearchResult(resourceResolver, path,
													regSearchPath.toString(), "ces"));
										} else {
											hits.addAll(getPageSearchResult(resourceResolver, path,
													egSearchPath.toString(), "ces"));
										}
									}

								} else {
									hits.add(getHashMapObj(resource));
								}
							}
						} else if ("ces".equals(group)) {
							LOG.info("WITHIN ces");
							for (Resource resource : searchresult) {
								String path = resource.getPath();
								if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
									if (path.startsWith(RegportalConstants.DAM_CES_DOCUMENTATION)
											|| path.contains("additionalresources")) {
										hits.add(getHashMapObj(resource));
									} else {
										if (RegportalConstants.REGULATORY_INSIGHT.equals(folderName)) {
											hits.addAll(getPageSearchResult(resourceResolver, path,
													regSearchPath.toString(), "easy"));
										} else {
											hits.addAll(getPageSearchResult(resourceResolver, path,
													cesSearchPath.toString(), "easy"));
										}
									}
								} else {
									hits.add(getHashMapObj(resource));
								}
							}
						} else {
							LOG.info("Group value is not identified by system. group={} ", group);
						}
					} else {
						LOG.info("Folder name does not exist for pages. folder={} ", folderName);
					}
				}
			} else {
				LOG.info("Searching in all pages of group={} ", group);
				if ("easygroup".equals(group)) {
					LOG.info("folder not found. group={}", group);
					for (Resource resource : searchresult) {
						String path = resource.getPath();
						if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
							if (path.startsWith(RegportalConstants.DAM_EASYGROUP_USERGUIDE) || path.contains("additionalresources")
									|| path.contains("releaseschedule")) {
								hits.add(getHashMapObj(resource));
							} else {
								hits.addAll(getPageSearchResult(resourceResolver, path, RegportalConstants.PAGE_BASE_PATH, "ces"));
							}
						} else {
							hits.add(getHashMapObj(resource));
						}
					}
				} else if ("ces".equals(group)) {
					LOG.info("No folder. group={}", group);
					for (Resource resource : searchresult) {
						String path = resource.getPath();
						if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
							if (path.startsWith(RegportalConstants.DAM_CES_DOCUMENTATION) || path.contains("additionalresources")) {
								hits.add(getHashMapObj(resource));
							} else {
								hits.addAll(getPageSearchResult(resourceResolver, path, RegportalConstants.PAGE_BASE_PATH, "easy"));
							}
						} else {
							hits.add(getHashMapObj(resource));
						}
					}
				} else if ("all".equals(group)) {
					LOG.info("No folder. group={} ", group);
					for (Resource resource : searchresult) {
						String path = resource.getPath();
						if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
							if (path.startsWith(RegportalConstants.DAM_EASYGROUP_USERGUIDE) || path.startsWith(RegportalConstants.DAM_CES_DOCUMENTATION)
									|| path.contains("additionalresources") || path.contains("releaseschedule")) {
								hits.add(getHashMapObj(resource));
							} else {
								hits.addAll(getPageSearchResult(resourceResolver, path, RegportalConstants.PAGE_BASE_PATH, null));
							}

						} else {
							hits.add(getHashMapObj(resource));
						}
					}

				} else {
					LOG.info("Group value is not identified by system. group={} ", group);
				}
			}
		} else {
			LOG.info("Initial Hit Size={} ", hits.size());
			if (!searchresult.isEmpty()) {
				for (Resource resource : searchresult) {
					String path = resource.getPath();
					if (path.startsWith(RegportalConstants.DAM_BASE_PATH)) {
						if (path.startsWith(RegportalConstants.DAM_CES_DOCUMENTATION) || path.startsWith(RegportalConstants.DAM_EASYGROUP_USERGUIDE)
								|| path.contains("additionalresources") || path.contains("releaseschedule")) {
							hits.add(getHashMapObj(resource));
						} else {
							hits.addAll(getPageSearchResult(resourceResolver, path, RegportalConstants.PAGE_BASE_PATH, null));
						}
					} else {
						hits.add(getHashMapObj(resource));
					}

				}
			}
		}
		LOG.info("Final Hit Size={} ", hits.size());
	}

	private HashMap<Resource, String> getHashMapObj(Resource res){
		HashMap<Resource, String> obj = new HashMap<Resource, String>();
		obj.put(res, res.getPath());
		return obj;
	}
	
	/**
	 * Final Search results are returned based on the keyword, group and folder values using this method.
	 */
	public Iterator<Resource> getFullTextMatchingResult(ResourceResolver resourceResolver, String keyword,
			String group, String folders) throws ParseException {
		LOG.info("getFullTextMachtingResult started");
		Iterator<Resource> resultItr = null;
		String query = StringUtils.EMPTY;
		if (StringUtils.isNotEmpty(keyword) && StringUtils.isEmpty(group) && StringUtils.isEmpty(folders)) {
			LOG.info("getFullTextMachtingResult : No group");
			query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword
					+ "') and isdescendantnode(a, '/content/dam/regportal') "
					+ "AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' "
					+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword
					+ "') and isdescendantnode(a, '/content/regportal/dashboard')";
		} else if (StringUtils.isNotEmpty(keyword) && StringUtils.isNotEmpty(group) && StringUtils.isEmpty(folders)) {
			if ("ces".equals(group)) {
				query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword
						+ "') and (isdescendantnode(a, '/content/dam/regportal/ces') or isdescendantnode(a, '/content/dam/regportal/api') or isdescendantnode(a, '/content/dam/regportal/others')) "
						+ "AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' AND [jcr:path] NOT like '/%/easy%' "
						+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword
						+ "') and isdescendantnode(a, '/content/regportal/dashboard') AND [jcr:path] NOT like '/%/easy%'";
			} else if ("easygroup".equals(group)) {
				query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword
						+ "') and (isdescendantnode(a, '/content/dam/regportal/easygroup') or isdescendantnode(a, '/content/dam/regportal/api') or isdescendantnode(a, '/content/dam/regportal/others')) "
						+ "AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' AND [jcr:path] NOT like '/%/ces%' "
						+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword
						+ "') and isdescendantnode(a, '/content/regportal/dashboard') AND [jcr:path] NOT like '/%/ces%'";
			} else {
				query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword
						+ "') and (isdescendantnode(a, '/content/dam/regportal') or isdescendantnode(a, '/content/dam/regportal/api') or isdescendantnode(a, '/content/dam/regportal/others')) "
						+ "AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' "
						+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword
						+ "') and isdescendantnode(a, '/content/regportal/dashboard')";
			}
		} else if (StringUtils.isNotEmpty(keyword) && StringUtils.isNotEmpty(group)
				&& StringUtils.isNotEmpty(folders)) {
			String paths[] = CommonSearchUtil.getDamFolderPath(group, folders);
			if ("ces".equals(group)) {
				query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword + "') and (" + paths[0]
						+ " or isdescendantnode(a, '/content/dam/regportal/api')) AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' AND [jcr:path] NOT like '/%/easy%' "
						+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword + "') and "
						+ paths[1] + " AND [jcr:path] NOT like '/%/easy%'";
			} else if ("easygroup".equals(group)) {
				query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword + "') and (" + paths[0]
						+ " or isdescendantnode(a, '/content/dam/regportal/api')) AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' AND [jcr:path] NOT like '/%/ces%' "
						+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword + "') and "
						+ paths[1] + " AND [jcr:path] NOT like '/%/ces%'";
			} else {
				query = "select [jcr:path] from [dam:Asset] as a where contains(*, '" + keyword + "') and (" + paths[0]
						+ " or isdescendantnode(a, '/content/dam/regportal/api')) AND [jcr:path] NOT like '/%/ez-unsearchable%' AND [jcr:path] NOT like '/%/ces-unsearchable%' "
						+ "union select [jcr:path] from [cq:Page] as a where contains(*, '" + keyword + "') and "
						+ paths[1];

			}
		} else {
			LOG.info("No condition matched to build the query.");
		}
		LOG.info("Final Query== {}", query);
		if (StringUtils.isNotEmpty(query)) {
			try {
				resultItr = resourceResolver.findResources(query, "sql");
			} catch (Exception e) {
				LOG.info("unknown exception", e);
			}

		} else {
			LOG.error("Keyword={} or group={} or folder={} is not matching", keyword, group, folders);
		}

		LOG.info("Results Found.");
		return resultItr;
	}

	


}

