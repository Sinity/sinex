use super::{Project, ProjectEvidence, project, project_evidence};

#[test]
fn project_roots_match_whole_path_components() {
    let projects = vec![
        Project {
            name: "outer".into(),
            roots: vec!["/src/tide".into()],
        },
        Project {
            name: "inner".into(),
            roots: vec!["/src/tide/tools".into()],
        },
        Project {
            name: "worktree".into(),
            roots: vec!["/realm/worktree/tide-*".into()],
        },
    ];
    assert_eq!(
        project("/src/tide/tools/bin", &projects).as_deref(),
        Some("inner")
    );
    let conflict = vec![
        Project {
            name: "a".into(),
            roots: vec!["/src/tide".into()],
        },
        Project {
            name: "b".into(),
            roots: vec!["/src/tide".into(), "/src/tide".into()],
        },
    ];
    assert_eq!(project("/src/tide", &conflict), None);
    assert_eq!(project("/src/tidepool", &projects), None);
    assert_eq!(project("/src/Tide", &projects), None);
    assert_eq!(
        project("/realm/worktree/tide-branch/crate", &projects).as_deref(),
        Some("worktree")
    );
    assert!(
        Project::validate_all(&[Project {
            name: "bad".into(),
            roots: vec!["/src/**".into()]
        }])
        .is_err()
    );
}
#[test]
fn deepest_root_wins_and_equal_depth_ties_are_unknown() {
    let mut projects = vec![
        Project {
            name: "short".into(),
            roots: vec!["/src/t*".into()],
        },
        Project {
            name: "long".into(),
            roots: vec!["/src/tide*".into()],
        },
    ];
    assert_eq!(project("/src/tidepool/tools", &projects), None);
    projects.push(Project {
        name: "nested".into(),
        roots: vec!["/src/tidepool/tools".into()],
    });
    assert_eq!(
        project("/src/tidepool/tools/bin", &projects).as_deref(),
        Some("nested")
    );
    projects.truncate(1);
    projects[0].roots.push("/src/tide*".into());
    projects[0].roots.push("/src/t*".into());
    assert_eq!(
        project("/src/tidepool/tools", &projects).as_deref(),
        Some("short")
    );
}
#[test]
fn wildcard_root_needs_the_component_but_not_extra_characters() {
    let projects = [Project {
        name: "tide".into(),
        roots: vec!["/src/tide*".into()],
    }];
    for path in [
        "/src/tide",
        "/src/tide/tools",
        "/src/tidepool",
        "/src/tidepool/tools",
    ] {
        assert_eq!(project(path, &projects).as_deref(), Some("tide"), "{path}");
    }
    let any = [Project {
        name: "child".into(),
        roots: vec!["/src/*".into()],
    }];
    assert_eq!(project("/src", &any), None);
    assert_eq!(project("/src/", &any), None);
    assert_eq!(project("/src/tide", &any).as_deref(), Some("child"));
}

#[test]
fn evidence_reports_why_a_cwd_has_no_project() {
    let projects = vec![
        Project {
            name: "tide".into(),
            roots: vec!["/work/tide".into()],
        },
        Project {
            name: "nested".into(),
            roots: vec!["/work/tide/tools".into()],
        },
        Project {
            name: "ambiguous".into(),
            roots: vec!["/work/tide/tools".into()],
        },
    ];
    assert_eq!(
        project_evidence(Some("/work/tide/src"), &projects),
        ProjectEvidence::Known {
            project: "tide".into()
        }
    );
    assert_eq!(
        project_evidence(Some("/work/tide-neighbor"), &projects),
        ProjectEvidence::Unconfigured {
            reason: "no_configured_root".into()
        }
    );
    assert_eq!(
        project_evidence(Some("/work/tide/tools"), &projects),
        ProjectEvidence::Unknown {
            reason: "ambiguous_roots".into()
        }
    );
    assert_eq!(
        project_evidence(None, &projects),
        ProjectEvidence::Unknown {
            reason: "missing_cwd".into()
        }
    );
    for cwd in [
        "relative/tide",
        "/work/tide/../other",
        "/work/tide/./src",
        "/work/tide\0",
    ] {
        assert_eq!(
            project_evidence(Some(cwd), &projects),
            ProjectEvidence::Unknown {
                reason: "invalid_cwd".into()
            }
        );
        assert_eq!(project(cwd, &projects), None);
    }
}
